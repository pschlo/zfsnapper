from __future__ import annotations
import logging
from typing import TypeAlias, Literal, TypeGuard, Any, overload
from dataclasses import dataclass
from enum import Enum, StrEnum
from itertools import pairwise, batched
from datetime import datetime

from zfsnapper.common.replication import ReplicationError
from zfsnapper.common.replication.send_receive import send_receive
from zfsnapper.common.command_utils import update_peerinfo, get_holds
from zfsnapper.common.parse_dataset_arg import ConnSpec
from zfsnapper.common.path import Path
from zfsnapper.common.sort import sortkey_snap_by_time
from zfsnapper.common.zfs import ZfsCli, Dataset, PeeringInfo, Snapshot, ZfsDatasetType, ZfsProperty, Pool, EXCLUDABLE_RECEIVE_PROPS
from zfsnapper.common.utils import space, is_subsequence
from zfsnapper.common.replication.utils import Direction, Peering


log = logging.getLogger(__name__)


_Sentinel = Enum("_Sentinel", {"NOT_SET": object()})
NOT_SET = _Sentinel.NOT_SET
NotSet: TypeAlias = Literal[_Sentinel.NOT_SET]


def is_set[T](value: T | NotSet) -> TypeGuard[T]:
    return value is not NOT_SET


class EncryptionMode(StrEnum):
    KEEP = 'keep'
    CLEAR = 'clear'


@dataclass
class DatasetSide:
    conn: ConnSpec
    cli: ZfsCli
    root: Path
    path: Path
    pool: Pool
    dataset: Dataset | NotSet = NOT_SET
    snaps: list[Snapshot] | NotSet = NOT_SET
    holds: dict[Snapshot, set[str]] | NotSet = NOT_SET
    holdtag: str | NotSet = NOT_SET
    base_snap: Snapshot | None | NotSet = NOT_SET


def replicate(source: DatasetSide, dest: DatasetSide, relpath: Path, rollback: bool, allow_init: bool, enc_mode: EncryptionMode, batch_size: int, localhost: str | None, log_indent: int = 0):
    def _s(level: int = 0):
        return space(log_indent + level)

    assert is_set(source.dataset) and is_set(source.snaps) and is_set(source.holds)
    if not source.snaps:
        raise ReplicationError(f"Source '{source.path}' has no snapshots")

    # Ensure sorting
    source.snaps.sort(key=sortkey_snap_by_time, reverse=True)

    # Flag to store whether we have just created/initialized the destination dataset
    just_created_dest: bool

    if not is_set(dest.dataset) or not is_set(dest.snaps) or not is_set(dest.holds):
        assert not is_set(dest.dataset) and not is_set(dest.snaps) and not is_set(dest.holds)

        # Dest dataset does not exist; cannot fetch snapshots.
        if not allow_init:
            raise ReplicationError(f"Destination dataset '{dest.path}' does not exist and will not be created", log_indent=log_indent)
        # Do initial send-receive to create dest dataset.
        transfer_initial(source, dest, snap=source.snaps[-1], enc_mode=enc_mode, log_indent=log_indent)

        # Fetch the newly created dataset and set base snaps
        source.base_snap = source.snaps[-1]
        dest.base_snap = source.base_snap.with_dataset(dest.path)
        dest.snaps = [dest.base_snap]
        dest.holds = {dest.base_snap: set()}
        dest.dataset = dest.cli.get_dataset(dest.path)

        # Determine holdtags
        source.holdtag = Peering(Direction.SEND, dest.dataset.guid).to_tag()
        dest.holdtag = Peering(Direction.RECEIVE, source.dataset.guid).to_tag()

        # Create holds
        source.cli.hold([source.base_snap.longname], source.holdtag)
        source.base_snap.num_holds += 1
        source.holds[source.base_snap].add(source.holdtag)

        dest.cli.hold([dest.base_snap.longname], dest.holdtag)
        dest.base_snap.num_holds += 1
        dest.holds[dest.base_snap].add(dest.holdtag)

        just_created_dest = True

    else:
        just_created_dest = False

        # Ensure sorting
        dest.snaps.sort(key=sortkey_snap_by_time, reverse=True)

        # Determine holdtags
        source.holdtag = Peering(Direction.SEND, dest.dataset.guid).to_tag()
        dest.holdtag = Peering(Direction.RECEIVE, source.dataset.guid).to_tag()

        # Determine base snap
        source.base_snap, dest.base_snap = find_latest_common(source, dest)

        # Optimize holds
        ensure_holds(source, dest, log_indent=log_indent)

        # figure out base index
        if source.base_snap is None or dest.base_snap is None:
            raise ReplicationError(f"Source '{source.path}' and destination '{dest.path}' have no common snapshots", log_indent=log_indent)
        if dest.base_snap.guid != dest.snaps[0].guid:
            raise ReplicationError(f"Destination '{dest.path}' has snapshots newer than latest common snapshot '{dest.base_snap.shortname}'", log_indent=log_indent)

        # Try to repair all snaps with unset tags.
        # For each snap that has tags set on source but UNSET on dest, set on dest.
        _src_guid_to_snap = {s.guid: s for s in source.snaps}
        for dest_snap in dest.snaps:
            repair_tags(
                dest_snap,
                src_guid_to_snap=_src_guid_to_snap,
                dest_cli=dest.cli,
                log_indent=log_indent
            )

        # Optionally rollback dest
        if rollback:
            log.info(_s() + f"Rolling back destination to latest snapshot")
            dest.cli.rollback(dest.snaps[0].longname)


    # Update peer information
    update_peerinfo(cli=source.cli, dataset=source.dataset, peerinfo=create_peering_info(dest, Direction.SEND), localhost=localhost)
    update_peerinfo(cli=dest.cli, dataset=dest.dataset, peerinfo=create_peering_info(source, Direction.RECEIVE), localhost=localhost)

    try:
        replicate_incrementally(source, dest, enc_mode=enc_mode, batch_size=batch_size, log_indent=log_indent)
    except ReplicationError as e:
        # Annotate that we have also sent an initial snapshot
        if just_created_dest:
            e.snaps_sent += 1
        raise e


def transfer_initial(source: DatasetSide, dest: DatasetSide, snap: Snapshot, enc_mode: EncryptionMode, log_indent: int = 0):
    """Perform a single initial send-receive, thereby creating the dest dataset. Also sets tags."""
    def _s(level: int = 0):
        return space(log_indent + level)

    assert is_set(source.dataset) and is_set(source.snaps)
    assert source.dataset.type in (ZfsDatasetType.FILESYSTEM, ZfsDatasetType.VOLUME)
    properties: dict[str, str] = {
        ZfsProperty.READONLY: 'on'
    }
    if source.dataset.type == ZfsDatasetType.FILESYSTEM:
        properties |= {
            ZfsProperty.ATIME: 'off',
            ZfsProperty.CANMOUNT: 'off',
            ZfsProperty.MOUNTPOINT: 'none'
        }

    log.info(_s() + f"Creating destination dataset by transferring oldest snapshot")
    _send_receive(
        source, dest,
        snap=snap,
        base=None,
        include_intermediates=False,
        enc_mode=enc_mode,
        override_props=properties,
        log_indent=log_indent + 1,
    )
    if snap.tags is not None:
        dest.cli.set_snapshot_tags(snap.with_dataset(dest.path).longname, snap.tags)


"""
- at all times, at least one common held anchor exists on both sides
- that anchor may lag behind the latest common snapshot by up to batch_size - 1 snapshots
"""

def replicate_incrementally(source: DatasetSide, dest: DatasetSide, enc_mode: EncryptionMode, batch_size: int, log_indent: int = 0):
    """Base snapshot must be held."""
    def _s(level: int = 0):
        return space(log_indent + level)

    assert is_set(source.dataset)
    assert is_set(source.snaps) and is_set(dest.snaps)
    assert is_set(source.base_snap) and is_set(dest.base_snap)
    assert is_set(source.holdtag) and is_set(dest.holdtag)
    assert source.base_snap is not None

    base_index = next(i for i, s in enumerate(source.snaps) if s.guid == source.base_snap.guid)

    # Determine sequence of planned transfers as (from_source_snap, to_source_snap) tuples
    # Default: transfer all source snapshots from common base to latest.
    transfer_sequence = list(pairwise(reversed(source.snaps[:base_index+1])))

    if not transfer_sequence:
        log.info(_s() + f"Already up to date")
        return

    # Check for timestamp conflicts
    check_timestamp_conflicts(source, dest, transfer_sequence=transfer_sequence, log_indent=log_indent)

    # Partition transfer sequence into batches
    transfer_batches = list(batched(transfer_sequence, batch_size))

    total = len(transfer_sequence)
    log.info(_s() + f"Destination is {total} snapshots behind")
    snaps_sent = 0
    for batch in transfer_batches:
        _progress = f"{snaps_sent+1}-{snaps_sent+len(batch)}/{total}" if len(batch) > 1 else f"{snaps_sent+1}/{total}"
        log.info(_s() + f"Transferring {len(batch)} snapshots [{_progress}]")
        try:
           _transfer_batch(
                batch,
                source,
                dest,
                enc_mode=enc_mode,
                log_indent=log_indent
            )
           snaps_sent += len(batch)
        except ReplicationError as e:
            # Annotate how many snapshots were sent successfully and re-raise
            e.snaps_sent = snaps_sent
            raise


def _transfer_batch(batch: tuple[tuple[Snapshot, Snapshot], ...], source: DatasetSide, dest: DatasetSide, enc_mode: EncryptionMode, log_indent: int = 0):
    """
    Transfer full batch.

    - First batch snap must be held
    - After, the last batch snap is held

    In worst case, tags are set only after entire batch has been sent.
    """
    assert is_set(source.snaps) and is_set(dest.snaps)
    assert is_set(source.holds) and is_set(dest.holds)
    assert is_set(source.dataset)
    assert is_set(source.holdtag) and is_set(dest.holdtag)

    def _s(level: int = 0):
        return space(log_indent + level)

    batch_first = batch[0][0]
    batch_last = batch[-1][1]

    # Determine whether snapshots in the batch are consecutive
    _snaps = [batch_first] + [p[1] for p in batch]
    is_consecutive = is_subsequence(list(reversed(_snaps)), source.snaps)

    if is_consecutive:
        # Can do single send_receive with include_intermediates=True
        _send_receive(source, dest, base=batch_first, snap=batch_last, include_intermediates=True, enc_mode=enc_mode, log_indent=log_indent+1)
        for _, snap in batch:
            # Determine corresponding dest snap, set tags, and store
            _dest_snap = snap.with_dataset(dest.path)
            if snap.tags is not None:
                dest.cli.set_snapshot_tags(_dest_snap.longname, snap.tags)
            dest.snaps.insert(0, _dest_snap)
            dest.holds.setdefault(_dest_snap, set())
    else:
        # Must send snapshots one-by-one
        for base, snap in batch:
            _send_receive(source, dest, base=base, snap=snap, include_intermediates=False, enc_mode=enc_mode, log_indent=log_indent+1)
            # Determine corresponding dest snap, set tags, and store
            _dest_snap = snap.with_dataset(dest.path)
            if snap.tags is not None:
                dest.cli.set_snapshot_tags(_dest_snap.longname, snap.tags)
            dest.snaps.insert(0, _dest_snap)
            dest.holds.setdefault(_dest_snap, set())

    # Determine first and last batch snapshot.
    batch_first_dest = next(iter(s for s in dest.snaps if s.guid == batch_first.guid))
    batch_last_dest = next(iter(s for s in dest.snaps if s.guid == batch_last.guid))

    # Update holds on first snap in batch
    source.cli.hold([batch_last.longname], source.holdtag)
    batch_last.num_holds += 1
    source.holds[batch_last].add(source.holdtag)

    dest.cli.hold([batch_last_dest.longname], dest.holdtag)
    batch_last_dest.num_holds += 1
    dest.holds[batch_last_dest].add(dest.holdtag)

    # Update holds on last snap in batch
    source.cli.release_hold([batch_first.longname], source.holdtag)
    batch_first.num_holds -= 1
    source.holds[batch_first].remove(source.holdtag)

    dest.cli.release_hold([batch_first_dest.longname], dest.holdtag)
    batch_first_dest.num_holds -= 1
    dest.holds[batch_first_dest].remove(dest.holdtag)


def _send_receive(
    source: DatasetSide,
    dest: DatasetSide,
    snap: Snapshot,
    base: Snapshot | None,
    include_intermediates: bool,
    enc_mode: EncryptionMode,
    override_props: dict[str, str] = {},
    log_indent: int = 0
) -> None:
    """Send a single snapshot from `source` to `dest`, while preserving custom zfsnapper tags."""
    assert is_set(source.dataset)
    send_receive(
        clis=(source.cli, dest.cli),
        dest_dataset=dest.path,
        snapshot=snap,
        base=base,
        raw=source.dataset.is_encrypted and enc_mode == EncryptionMode.KEEP,
        include_intermediates=include_intermediates,
        send_props=False,  # Currently incompatible with raw send
        # no_preserve_encryption=source.dataset.is_encrypted and enc_mode == EncryptionMode.CLEAR,  # not yet widely available
        # exclude_props=EXCLUDABLE_RECEIVE_PROPS - {ZfsProperty.CUSTOM_TAGS},  # exclude all known properties except custom tags,
        override_props=override_props,
        log_indent=log_indent
    )


def create_peering_info(side: DatasetSide, direction: Direction):
    assert is_set(side.dataset)
    return PeeringInfo(
        peering=Peering(
            direction=direction,
            guid=side.dataset.guid
        ),
        last_used=datetime.now(),
        path=side.path,
        pool_guid=side.pool.guid,
        host=side.conn
    )


def check_timestamp_conflicts(source: DatasetSide, dest: DatasetSide, transfer_sequence: list[tuple[Snapshot, Snapshot]], log_indent: int = 0):
    # Find snapshot that cannot be transferred because their timestamp equals their predecessor
    for a, b in transfer_sequence:
        if a.timestamp == b.timestamp:
            # Snapshot B cannot be sent
            raise ReplicationError(
                f"Cannot transfer snapshots from '{source.path}' to '{dest.path}': "
                f"snapshot '{b.shortname}' shares timestamp with predecessor '{a.shortname}'",
                log_indent=log_indent
            )


def repair_tags(dest_snap: Snapshot, src_guid_to_snap: dict[int, Snapshot], dest_cli: ZfsCli, log_indent: int = 0):
    def _s(level: int = 0):
        return space(log_indent + level)

    if dest_snap.tags is not None:
        # Tags are already set
        return

    src_snap = src_guid_to_snap.get(dest_snap.guid)
    if src_snap is None:
        # There is no corresponding source snapshot to copy tags from
        return

    if src_snap.tags is None:
        # Source snapshot does not have tags set
        return

    log.info(_s() + f"Adding {len(src_snap.tags)} missing tags to destination snapshot: {dest_snap.shortname}")
    dest_cli.set_snapshot_tags(dest_snap.longname, src_snap.tags)
    dest_snap.tags = frozenset(src_snap.tags)


def ensure_holds(source: DatasetSide, dest: DatasetSide, log_indent: int = 0):
    """Ensures the latest common snapshot is held on both sides. Removes all other peer holdtags.

    After completion, one of these is true:
    - There are no holdtags on either side, since there was no common snapshot
    - There is exactly one holdtag on each side, on the latest common snapshot
    """
    assert is_set(source.snaps) and is_set(dest.snaps)
    assert is_set(source.holds) and is_set(dest.holds)
    assert is_set(source.holdtag) and is_set(dest.holdtag)
    assert is_set(source.base_snap) and is_set(dest.base_snap)

    def _s(level: int = 0):
        return space(log_indent + level)

    if source.base_snap is None or dest.base_snap is None:
        # Remove all peer holdtags
        release_snaps = (
            source.snaps,
            dest.snaps
        )
        _release_holds((source.cli, dest.cli), release_snaps, (source.holdtag, dest.holdtag), current_holdtags=(source.holds, dest.holds), log_indent=log_indent)
        return

    # Ensure latest common snap is held
    if source.holdtag not in source.holds[source.base_snap]:
        log.info(_s() + f"Creating hold for latest common snapshot '{source.base_snap.shortname}' on source")
        source.cli.hold([source.base_snap.longname], tag=source.holdtag)
        source.base_snap.num_holds += 1
        source.holds[source.base_snap].add(source.holdtag)
    if dest.holdtag not in dest.holds[dest.base_snap]:
        log.info(_s() + f"Creating hold for latest common snapshot '{dest.base_snap.shortname}' on destination")
        dest.cli.hold([dest.base_snap.longname], tag=dest.holdtag)
        dest.base_snap.num_holds += 1
        dest.holds[dest.base_snap].add(dest.holdtag)

    # Remove all other holdtags
    release_snaps = (
        [s for s in source.snaps if s.guid != source.base_snap.guid],
        [s for s in dest.snaps if s.guid != dest.base_snap.guid]
    )
    _release_holds((source.cli, dest.cli), release_snaps, (source.holdtag, dest.holdtag), current_holdtags=(source.holds, dest.holds), log_indent=log_indent)


def find_latest_common(source: DatasetSide, dest: DatasetSide) -> tuple[Snapshot, Snapshot] | tuple[None, None]:
    """Finds the latest snapshot that exists on both sides."""
    assert is_set(source.snaps) and is_set(dest.snaps)

    guid_to_snap = (
        {s.guid: s for s in source.snaps},
        {s.guid: s for s in dest.snaps}
    )
    common_guids = guid_to_snap[0].keys() & guid_to_snap[1].keys()
    if not common_guids:
        return (None, None)

    # For determinism, sort by GUID if timestamps are equal.
    # Just to be safe, ensure the snapshot is actually the latest common snapshot on both sides.
    _latest_guid_src = max(common_guids, key=lambda g: (guid_to_snap[0][g].timestamp, g))
    _latest_guid_dest = max(common_guids, key=lambda g: (guid_to_snap[1][g].timestamp, g))
    assert _latest_guid_src == _latest_guid_dest
    latest_guid = _latest_guid_src
    latest_common_snap = (guid_to_snap[0][latest_guid], guid_to_snap[1][latest_guid])
    log.debug(f"Latest common snapshot is '{latest_common_snap[0].longname}' on source, '{latest_common_snap[1].longname}' on destination")

    return latest_common_snap



def _release_holds(
    clis: tuple[ZfsCli, ZfsCli],
    snaps: tuple[list[Snapshot], list[Snapshot]],
    release_holdtags: tuple[str, str],
    current_holdtags: tuple[dict[Snapshot, set[str]], dict[Snapshot, set[str]]],
    log_indent: int = 0
):
    def _s(level: int = 0):
        return space(log_indent + level)

    # Filter for snaps that have the holdtags
    release_snaps = (
        [s for s in snaps[0] if release_holdtags[0] in current_holdtags[0][s]],
        [s for s in snaps[1] if release_holdtags[1] in current_holdtags[1][s]],
    )
    if release_snaps[0]:
        log.info(_s() + f"Releasing {len(release_snaps[0])} obsolete holds on source")
    if release_snaps[1]:
        log.info(_s() + f"Releasing {len(release_snaps[1])} obsolete holds on destination")

    clis[0].release_hold([s.longname for s in release_snaps[0]], release_holdtags[0])
    for s in release_snaps[0]:
        s.num_holds -= 1
        current_holdtags[0][s].remove(release_holdtags[0])

    clis[1].release_hold([s.longname for s in release_snaps[1]], release_holdtags[1])
    for s in release_snaps[1]:
        s.num_holds -= 1
        current_holdtags[1][s].remove(release_holdtags[1])

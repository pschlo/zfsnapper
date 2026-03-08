from __future__ import annotations
import logging
from typing import TypeAlias, Literal, TypeGuard, Any, overload
from dataclasses import dataclass
from enum import Enum
from itertools import pairwise
from datetime import datetime

from zfsnappr.common.replication import ReplicationError
from zfsnappr.common.replication.send_receive import send_receive
from zfsnappr.common.command_utils import update_peerinfo, get_holds
from zfsnappr.common.parse_dataset_arg import ConnSpec
from zfsnappr.common.path import Path
from zfsnappr.common.sort import sortkey_snap_by_time
from zfsnappr.common.zfs import ZfsCli, Dataset, PeerInfo, Snapshot, ZfsDatasetType, ZfsProperty, Pool
from zfsnappr.common.utils import space


log = logging.getLogger(__name__)


_Sentinel = Enum("_Sentinel", {"NOT_SET": object()})
NOT_SET = _Sentinel.NOT_SET
NotSet: TypeAlias = Literal[_Sentinel.NOT_SET]


def is_set[T](value: T | NotSet) -> TypeGuard[T]:
    return value is not NOT_SET


@dataclass
class DatasetSide:
    conn: ConnSpec
    cli: ZfsCli
    root: Path
    path: Path
    pool: Pool
    dataset: Dataset | NotSet = NOT_SET
    snaps: list[Snapshot] | NotSet = NOT_SET
    holdtag: str | NotSet = NOT_SET
    base_snap: Snapshot | None | NotSet = NOT_SET


def replicate(source: DatasetSide, dest: DatasetSide, relpath: Path, rollback: bool, allow_init: bool, log_indent: int = 0):
    def _s(level: int = 0):
        return space(log_indent + level)

    assert is_set(source.dataset) and is_set(source.snaps)

    # Ensure sorting
    source.snaps.sort(key=sortkey_snap_by_time, reverse=True)

    if not is_set(dest.dataset) or not is_set(dest.snaps):
        assert not is_set(dest.dataset) and not is_set(dest.snaps)

        # Dest dataset does not exist; cannot fetch snapshots.
        if not allow_init:
            raise ReplicationError(f"Destination dataset '{dest.path}' does not exist and will not be created", log_indent=log_indent)
        # Do initial send-receive to create dest dataset.
        transfer_initial(source, dest, snapshot=source.snaps[-1], log_indent=log_indent)

        # Fetch the newly created dataset and set base snaps
        source.base_snap = source.snaps[-1]
        dest.base_snap = source.base_snap.with_dataset(dest.path)
        dest.snaps = [dest.base_snap]
        dest.dataset = dest.cli.get_dataset(dest.path)

        # Determine holdtags
        source.holdtag = f'zfsnappr-sendbase-{dest.dataset.guid}'
        dest.holdtag = f'zfsnappr-recvbase-{source.dataset.guid}'

        # Create holds
        source.cli.hold([source.base_snap.longname], source.holdtag)
        source.base_snap.num_holds += 1
        dest.cli.hold([dest.base_snap.longname], dest.holdtag)
        dest.base_snap.num_holds += 1

    else:
        # Ensure sorting
        dest.snaps.sort(key=sortkey_snap_by_time, reverse=True)

        # Determine holdtags
        source.holdtag = f'zfsnappr-sendbase-{dest.dataset.guid}'
        dest.holdtag = f'zfsnappr-recvbase-{source.dataset.guid}'

        # Determine base snap
        source.base_snap, dest.base_snap = determine_latest_common(source, dest)

        # Check holds
        ensure_holds(source, dest, log_indent=log_indent)

        # figure out base index
        if source.base_snap is None or dest.base_snap is None:
            raise ReplicationError(f"Source '{source.path}' and destination '{dest.path}' have no common snapshots", log_indent=log_indent)
        if dest.base_snap.guid != dest.snaps[0].guid:
            raise ReplicationError(f"Destination '{dest.path}' has snapshots newer than latest common snapshot '{dest.base_snap.shortname}'", log_indent=log_indent)

        # Check base snap tags
        check_base_snap_tags(source, dest, log_indent=log_indent)

        # Optionally rollback dest
        if rollback:
            log.info(_s() + f"Rolling back destination to latest snapshot")
            dest.cli.rollback(dest.snaps[0].longname)


    # Update peer information
    update_peerinfo(cli=source.cli, dataset=source.dataset, peer=to_peer(dest))
    update_peerinfo(cli=dest.cli, dataset=dest.dataset, peer=to_peer(source))

    replicate_incrementally(source, dest, log_indent=log_indent)


def transfer_initial(source: DatasetSide, dest: DatasetSide, snapshot: Snapshot, log_indent: int = 0):
    """Perform a single initial send-receive, thereby creating the dest dataset."""
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
    send_receive(
        clis=(source.cli, dest.cli),
        dest_dataset=dest.path,
        snapshot=snapshot,
        base=None,
        properties=properties,
        log_indent=log_indent + 1
    )


def replicate_incrementally(source: DatasetSide, dest: DatasetSide, log_indent: int = 0):
    """Base snapshot must be held."""
    def _s(level: int = 0):
        return space(log_indent + level)

    assert is_set(source.snaps) and is_set(dest.snaps)
    assert is_set(source.base_snap) and is_set(dest.base_snap)
    assert is_set(source.holdtag) and is_set(dest.holdtag)
    assert source.base_snap is not None

    base_index = next(i for i, s in enumerate(source.snaps) if s.guid == source.base_snap.guid)

    # Determine sequence of source snapshots to transfer.
    # Default: transfer all source snapshots from common base to latest.
    transfer_sequence = list(reversed(source.snaps[:base_index+1]))

    # must at least contain a base snapshot
    assert transfer_sequence
    if len(transfer_sequence) <= 1:
        log.info(_s() + f"Already up to date")
        return

    # Check for timestamp conflicts
    check_timestamp_conflicts(source, dest, transfer_sequence=transfer_sequence, log_indent=log_indent)

    total = len(transfer_sequence) - 1
    log.info(_s() + f"Destination is {total} snapshots behind")
    for i, (base, snap) in enumerate(pairwise(transfer_sequence)):
        log.info(_s() + f"Transferring snapshot [{i+1}/{total}]: {snap.shortname}")

        # Transfer snapshot
        send_receive(
            clis=(source.cli, dest.cli),
            dest_dataset=dest.path,
            snapshot=snap,
            base=base,
            log_indent=log_indent + 1
        )

        # Determine dest snaps
        dest_base = base.with_dataset(dest.path)
        dest_snap = snap.with_dataset(dest.path)
        dest.snaps.insert(0, dest_snap)

        # Update holds
        source.cli.hold([snap.longname], source.holdtag)
        snap.num_holds += 1
        dest.cli.hold([dest_snap.longname], dest.holdtag)
        dest_snap.num_holds += 1
        source.cli.release_hold([base.longname], source.holdtag)
        base.num_holds -= 1
        dest.cli.release_hold([dest_base.longname], dest.holdtag)
        dest_base.num_holds -= 1

    dest.snaps = [s.with_dataset(dest.path) for s in reversed(transfer_sequence[1:])] + dest.snaps


def to_peer(side: DatasetSide):
    assert is_set(side.dataset)
    return PeerInfo(
        last_used=datetime.now(),
        guid=side.dataset.guid,
        path=side.path,
        pool_guid=side.pool.guid,
        host=side.conn
    )


def check_timestamp_conflicts(source: DatasetSide, dest: DatasetSide, transfer_sequence: list[Snapshot], log_indent: int = 0):
    # Find snapshot that cannot be transferred because their timestamp equals their predecessor
    for i, (a, b) in enumerate(pairwise(transfer_sequence)):
        if a.timestamp == b.timestamp:
            # Snapshot B cannot be sent
            raise ReplicationError(
                f"Cannot transfer snapshots from '{source.path}' to '{dest.path}': "
                f"snapshot '{b.shortname}' shares timestamp with predecessor '{a.shortname}'",
                log_indent=log_indent
            )


def check_base_snap_tags(source: DatasetSide, dest: DatasetSide, log_indent: int = 0):
    def _s(level: int = 0):
        return space(log_indent + level)

    assert is_set(source.base_snap) and is_set(dest.base_snap)
    assert source.base_snap is not None and dest.base_snap is not None

    # Ensure base snapshot on dest has correct tags; this may help if previous replication was aborted before tags could be set
    if (_src_tags := source.base_snap.tags) is not None:
        _dest_tags = dest.base_snap.tags or set()
        if _missing := _src_tags - _dest_tags:
            log.info(_s() + f"Adding {len(_missing)} missing tags to base snapshot '{dest.base_snap.shortname}' on destination")
            dest_tags = frozenset(_dest_tags | _missing)
            dest.cli.set_snapshot_tags(dest.base_snap.longname, dest_tags)
            dest.base_snap.tags = dest_tags


def ensure_holds(source: DatasetSide, dest: DatasetSide, log_indent: int = 0):
    """Uses source.base_snap and dest.base_snap."""
    assert is_set(source.snaps) and is_set(dest.snaps)
    assert is_set(source.holdtag) and is_set(dest.holdtag)
    assert is_set(source.base_snap) and is_set(dest.base_snap)

    """Ensures the latest common snapshot is held on both sides. Removes all other peer holdtags.

    After completion, one of these is true:
    1. There are no holdtags on either side, since there was no common snapshot
    2. There is exactly one holdtag on each side, on the latest common snapshot
    """
    def _s(level: int = 0):
        return space(log_indent + level)

    # Get holds
    holds = (
        get_holds(source.cli, source.snaps),
        get_holds(dest.cli, dest.snaps)
    )

    if source.base_snap is None or dest.base_snap is None:
        # Remove all peer holdtags
        release_snaps = (
            source.snaps,
            dest.snaps
        )
        _release_holds((source.cli, dest.cli), release_snaps, (source.holdtag, dest.holdtag), current_holdtags=holds, log_indent=log_indent)
        return

    # Ensure latest common snap is held
    if source.holdtag not in holds[0][source.base_snap]:
        log.info(_s() + f"Creating hold for latest common snapshot '{source.base_snap.shortname}' on source")
        source.cli.hold([source.base_snap.longname], tag=source.holdtag)
        source.base_snap.num_holds += 1
    if dest.holdtag not in holds[1][dest.base_snap]:
        log.info(_s() + f"Creating hold for latest common snapshot '{dest.base_snap.shortname}' on destination")
        dest.cli.hold([dest.base_snap.longname], tag=dest.holdtag)
        dest.base_snap.num_holds += 1

    # Remove all other holdtags
    release_snaps = (
        [s for s in source.snaps if s.guid != source.base_snap.guid],
        [s for s in dest.snaps if s.guid != dest.base_snap.guid]
    )
    _release_holds((source.cli, dest.cli), release_snaps, (source.holdtag, dest.holdtag), current_holdtags=holds, log_indent=log_indent)


def determine_latest_common(source: DatasetSide, dest: DatasetSide) -> tuple[Snapshot, Snapshot] | tuple[None, None]:
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

    clis[1].release_hold([s.longname for s in release_snaps[1]], release_holdtags[1])
    for s in release_snaps[1]:
        s.num_holds -= 1

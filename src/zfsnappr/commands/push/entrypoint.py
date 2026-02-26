from __future__ import annotations
import logging
from typing import cast
from dataclasses import dataclass
from itertools import pairwise
from datetime import datetime

from zfsnappr.common.replication import ReplicationError
from zfsnappr.common.replication.send_receive_snap import _send_receive
from zfsnappr.common.resolve_datasets import ResolvedDatasets, create_zfs_cli, resolve_conn_datasets
from zfsnappr.common.command_utils import resolve_dataset_args, fetch_snaps, update_peer, get_holds
from zfsnappr.common.parse_dataset_arg import parse_dataset_arg, DatasetSpec, ConnSpec
from zfsnappr.common.path import Path
from zfsnappr.common.sort import sortkey_snap_by_time
from zfsnappr.common.zfs import ZfsCli, Dataset, Peer, Snapshot, ZfsDatasetType, ZfsProperty
from zfsnappr.common.utils import group_by, combine_dicts, space
from .args import Args


log = logging.getLogger(__name__)


def entrypoint(args: Args) -> None:
    src_resolved = resolve_dataset_args(args)
    dest_spec = parse_dataset_arg(args.dest)
    dest_cli = create_zfs_cli(dest_spec.conn)

    _first = True
    for conn, (datasets, cli) in src_resolved.items():
        if not _first:
            log.info("")
        _first = False

        push_conn(
            src_cli=cli,
            src_datasets=datasets,
            dest_cli=dest_cli,
            dest_root=dest_spec.dataset,
            allow_initialize=args.init,
            rollback=args.rollback,
            src_conn=conn,
            dst_conn=dest_spec.conn
        )


def push_conn(
    src_cli: ZfsCli,
    dest_cli: ZfsCli,
    src_datasets: ResolvedDatasets,
    dest_root: Path,
    allow_initialize: bool,
    rollback: bool,
    src_conn: ConnSpec,
    dst_conn: ConnSpec
):
    """
    Push MULTIPLE source datasets to SINGLE dest dataset
    """
    def _s(level: int = 0):
        return space(level)

    # Find longest common src prefix; may be empty path
    src_root = src_datasets.p.deepest_common_ancestor
    log.info(f"Replicating: {src_conn}/{src_root} → {dst_conn}/{dest_root}")

    # Create matching of source dataset to dest dataset
    srcpath_to_destpath = {
        src_path: dest_root / src_path.relative_to(src_root)
        for src_path in sorted(src_datasets.p.matched, key=lambda p: p.depth)
    }

    # Determine corresponding dest datasets
    # Some expected dest datasets may be missing
    dest_datasets = resolve_conn_datasets(
        cli=dest_cli,
        include_exact=srcpath_to_destpath.values()
    )

    # Determine missing dest datasets
    missing_dest_paths = set(srcpath_to_destpath.values()) - dest_datasets.p.matched

    # Fetch all snapshots.
    srcpath_to_snaps = group_by(
        fetch_snaps(cli=src_cli, datasets=src_datasets),
        lambda s: s.dataset,
        ensure_keys=src_datasets.p.matched
    )
    destpath_to_snaps = group_by(
        fetch_snaps(cli=dest_cli, datasets=dest_datasets),
        lambda s: s.dataset,
        ensure_keys=dest_datasets.p.matched
    )





    # Replicate dataset-by-dataset
    is_error = False
    for srcpath, destpath in srcpath_to_destpath.items():
        relpath = srcpath.relative_to(src_root)
        source = DatasetSide(
            conn=src_conn,
            cli=src_cli,
            root=src_root,
            path=srcpath,
            content=DatasetContent(
                dataset=src_datasets.path_to_dataset[srcpath],
                snaps=srcpath_to_snaps[srcpath]
            )
        )
        dest = DatasetSide(
            conn=dst_conn,
            cli=dest_cli,
            root=dest_root,
            path=destpath,
            content=DatasetContent(
                dataset=dest_datasets.path_to_dataset[destpath],
                snaps=destpath_to_snaps[destpath]
            ) if destpath not in missing_dest_paths else None
        )
        try:
            log.info(_s(1) + f"Checking dataset: ~{f'/{relpath}' if relpath else ''}")
            replicate(source, dest, relpath=relpath, rollback=rollback, allow_init=allow_initialize, log_indent=2)
        except ReplicationError as e:
            is_error = True
            log.error(space(e.log_indent) + str(e))

    if is_error:
        raise ReplicationError(f"Replication failed for one or more datasets")



@dataclass
class DatasetContent:
    dataset: Dataset
    snaps: list[Snapshot]


@dataclass
class DatasetSide:
    conn: ConnSpec
    cli: ZfsCli
    root: Path
    path: Path
    content: DatasetContent | None = None
    holdtag: str | None = None
    base_snap: Snapshot | None = None


def replicate(source: DatasetSide, dest: DatasetSide, relpath: Path, rollback: bool, allow_init: bool, log_indent: int = 0):
    def _s(level: int = 0):
        return space(log_indent + level)

    assert source.content is not None

    # Ensure sorting
    source.content.snaps.sort(key=sortkey_snap_by_time, reverse=True)

    if dest.content is None:
        # Dest dataset does not exist; cannot fetch snapshots.
        if not allow_init:
            raise ReplicationError(f"Destination dataset '{dest.path}' does not exist and will not be created", log_indent=2)
        # Do initial send-receive to create dest dataset.
        transfer_initial(source, dest, snapshot=source.content.snaps[-1])

        # Fetch the newly created dataset and set base snaps
        source.base_snap = source.content.snaps[-1]
        dest.base_snap = source.base_snap.with_dataset(dest.path)
        dest.content = DatasetContent(
            dataset=dest.cli.get_dataset(dest.path),
            snaps=[dest.base_snap]
        )

        # Update peer information
        update_peer(cli=source.cli, dataset=source.content.dataset, peer=to_peer(dest))
        update_peer(cli=dest.cli, dataset=dest.content.dataset, peer=to_peer(source))

        # Determine holdtags
        source.holdtag = f'zfsnappr-sendbase-{dest.content.dataset.guid}'
        dest.holdtag = f'zfsnappr-recvbase-{source.content.dataset.guid}'

        # Create holds
        source.cli.hold([source.base_snap.longname], source.holdtag)
        source.base_snap.holds += 1
        dest.cli.hold([dest.base_snap.longname], dest.holdtag)
        dest.base_snap.holds += 1

    else:
        # Ensure sorting
        dest.content.snaps.sort(key=sortkey_snap_by_time, reverse=True)

        # Update peer information
        update_peer(cli=source.cli, dataset=source.content.dataset, peer=to_peer(dest))
        update_peer(cli=dest.cli, dataset=dest.content.dataset, peer=to_peer(source))

        # Determine holdtags
        source.holdtag = f'zfsnappr-sendbase-{dest.content.dataset.guid}'
        dest.holdtag = f'zfsnappr-recvbase-{source.content.dataset.guid}'

        # Determine base snap
        source.base_snap, dest.base_snap = determine_latest_common(source, dest)

        # Check holds
        ensure_holds(source, dest)

        # figure out base index
        if source.base_snap is None or dest.base_snap is None:
            raise ReplicationError(f"Source '{source.path}' and destination '{dest.path}' have no common snapshot", log_indent=log_indent)
        if dest.base_snap.guid != dest.content.snaps[0].guid:
            raise ReplicationError(f"Destination '{dest.path}' has snapshots newer than latest common snapshot '{dest.base_snap.shortname}'", log_indent=log_indent)

        # Check base snap tags
        check_base_snap_tags(source, dest)

        # Optionally rollback dest
        if rollback:
            log.info(_s() + f"Rolling back destination to latest snapshot")
            dest.cli.rollback(dest.content.snaps[0].longname)

    transfer_incremental(source, dest)




def transfer_initial(source: DatasetSide, dest: DatasetSide, snapshot: Snapshot, log_indent: int = 0):
    """Perform a single initial send-receive, thereby creating the dest dataset."""
    assert source.content
    assert source.content.dataset.type in (ZfsDatasetType.FILESYSTEM, ZfsDatasetType.VOLUME)
    properties: dict[str, str] = {
        ZfsProperty.READONLY: 'on'
    }
    if source.content.dataset.type == ZfsDatasetType.FILESYSTEM:
        properties |= {
            ZfsProperty.ATIME: 'off',
            ZfsProperty.CANMOUNT: 'off',
            ZfsProperty.MOUNTPOINT: 'none'
        }
    _send_receive(
        clis=(source.cli, dest.cli),
        dest_dataset=dest.path,
        snapshot=snapshot,
        base=None,
        properties=properties,
        log_indent=log_indent
    )


def transfer_incremental(source: DatasetSide, dest: DatasetSide, log_indent: int = 0):
    """Base snapshot must be held."""
    def _s(level: int = 0):
        return space(log_indent + level)

    assert source.content and dest.content
    assert source.base_snap and dest.base_snap
    assert source.holdtag is not None and dest.holdtag is not None

    base_index = next(i for i, s in enumerate(source.content.snaps) if s.guid == source.base_snap.guid)

    # Determine sequence of source snapshots to transfer.
    # Default: transfer all source snapshots from common base to latest.
    transfer_sequence = list(reversed(source.content.snaps[:base_index+1]))

    # must at least contain a base snapshot
    assert transfer_sequence
    if len(transfer_sequence) <= 1:
        log.info(_s() + f"Already up to date")
        return

    # Check for timestamp conflicts
    check_timestamp_conflicts(source, dest, transfer_sequence=transfer_sequence)

    total = len(transfer_sequence) - 1
    log.info(_s() + f"Destination is {total} snapshots behind")
    for i, (base, snap) in enumerate(pairwise(transfer_sequence)):
        log.info(_s() + f"Transferring snapshot [{i+1}/{total}]: {snap.shortname}")

        # Transfer snapshot
        _send_receive(
            clis=(source.cli, dest.cli),
            dest_dataset=dest.path,
            snapshot=snap,
            base=base,
            log_indent=log_indent + 1
        )

        # Determine dest snaps
        dest_base = base.with_dataset(dest.path)
        dest_snap = snap.with_dataset(dest.path)
        dest.content.snaps.insert(0, dest_snap)

        # Update holds
        source.cli.hold([snap.longname], source.holdtag)
        dest.cli.hold([dest_snap.longname], dest.holdtag)
        source.cli.release_hold([base.longname], source.holdtag)
        dest.cli.release_hold([dest_base.longname], dest.holdtag)

    dest.content.snaps = [s.with_dataset(dest.path) for s in reversed(transfer_sequence[1:])] + dest.content.snaps



def to_peer(side: DatasetSide):
    assert side.content is not None
    return Peer(
        last_used=datetime.now(),
        guid=side.content.dataset.guid,
        path=side.path,
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

    assert source.base_snap and dest.base_snap
    # Ensure base snapshot on dest has correct tags; this may help if previous replication was aborted before tags could be set
    if (_src_tags := source.base_snap.tags) is not None:
        _dest_tags = dest.base_snap.tags or set()
        if _missing := _src_tags - _dest_tags:
            log.info(_s() + f"Adding {len(_missing)} missing tags to base snapshot '{dest.base_snap.shortname}' on destination")
            dest_tags = frozenset(_dest_tags | _missing)
            dest.cli.set_snapshot_tags(dest.base_snap.longname, dest_tags)
            dest.base_snap.tags = dest_tags


def ensure_holds(source: DatasetSide, dest: DatasetSide, log_indent: int = 0):
    assert source.content and dest.content
    assert source.holdtag and dest.holdtag
    # assert source.base_snap is not None and dest.base_snap is not None

    """Ensures the latest common snapshot is held on both sides. Removes all other peer holdtags.

    After completion, one of these is true:
    1. There are no holdtags on either side, since there was no common snapshot
    2. There is exactly one holdtag on each side, on the latest common snapshot
    """
    def _s(level: int = 0):
        return space(log_indent + level)

    # Get holds
    holds = (
        get_holds(source.cli, source.content.snaps),
        get_holds(dest.cli, dest.content.snaps)
    )

    if source.base_snap is None or dest.base_snap is None:
        # Remove all peer holdtags
        release_snaps = (
            source.content.snaps,
            dest.content.snaps
        )
        _release_holds((source.cli, dest.cli), release_snaps, (source.holdtag, dest.holdtag), current_holdtags=holds, log_indent=log_indent)
        return

    # Ensure latest common snap is held
    if source.holdtag not in holds[0][source.base_snap]:
        log.info(_s() + f"Creating hold for latest common snapshot '{source.base_snap.shortname}' on source")
        source.cli.hold([source.base_snap.longname], tag=source.holdtag)
    if dest.holdtag not in holds[1][dest.base_snap]:
        log.info(_s() + f"Creating hold for latest common snapshot '{dest.base_snap.shortname}' on destination")
        dest.cli.hold([dest.base_snap.longname], tag=dest.holdtag)

    # Remove all other holdtags
    release_snaps = (
        [s for s in source.content.snaps if s.guid != source.base_snap.guid],
        [s for s in dest.content.snaps if s.guid != dest.base_snap.guid]
    )
    _release_holds((source.cli, dest.cli), release_snaps, (source.holdtag, dest.holdtag), current_holdtags=holds, log_indent=log_indent)





def determine_latest_common(source: DatasetSide, dest: DatasetSide) -> tuple[Snapshot, Snapshot] | tuple[None, None]:
    """Finds the latest snapshot that exists on both sides."""
    assert source.content and dest.content

    guid_to_snap = (
        {s.guid: s for s in source.content.snaps},
        {s.guid: s for s in dest.content.snaps}
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
    clis[1].release_hold([s.longname for s in release_snaps[1]], release_holdtags[1])

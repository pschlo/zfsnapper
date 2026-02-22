from __future__ import annotations
from typing import Optional, cast
from collections.abc import Collection
import logging
from itertools import pairwise

from zfsnappr.common.zfs import Snapshot, ZfsCli, ZfsProperty, Dataset
from zfsnappr.common.replication.exception import ReplicationError
from zfsnappr.common.path import Path
from zfsnappr.common.sort import sort_snaps_by_time
from zfsnappr.common.utils import space

from .send_receive_snap import send_receive_incremental, send_receive_initial

log = logging.getLogger(__name__)


def holdtag_src(dest_dataset: Dataset):
    return f'zfsnappr-sendbase-{dest_dataset.guid}'

def holdtag_dest(src_dataset: Dataset):
    return f'zfsnappr-recvbase-{src_dataset.guid}'


def replicate_snaps_initial(
    source_cli: ZfsCli,
    source_dataset: Dataset,
    source_snaps: Collection[Snapshot],
    dest_dataset: Path,
    dest_cli: ZfsCli,
    dest_root: Path,
    log_indent: int = 0
):
    def _s(level: int = 0):
        return space(log_indent + level)

    # sorting is required
    source_snaps = sort_snaps_by_time(source_snaps, reverse=True)

    _rel_dest = dest_dataset.relative_to(dest_root)
    log.info(_s() + f"Creating destination dataset by transferring oldest snapshot")
    initial_src_snap = source_snaps[-1]
    send_receive_initial(
        clis=(source_cli, dest_cli),
        dest_dataset=dest_dataset,
        source_dataset_type=source_dataset.type,
        snapshot=initial_src_snap,
        holdtags=(holdtag_src, holdtag_dest),
        log_indent=log_indent + 1
    )

    # Continue with incremental replication
    initial_dest_snap = initial_src_snap.with_dataset(dest_dataset)
    # Temporary hack to ensure holds get respected
    initial_src_snap.holds += 1
    initial_dest_snap.holds += 1
    replicate_snaps_incremental(
        source_cli=source_cli,
        source_snaps=source_snaps,
        dest_cli=dest_cli,
        dest_snaps=[initial_dest_snap],
        rollback=False,  # we just created the dataset; rollback would be obsolete
        log_indent=log_indent
    )


# TODO: raw send for encrypted datasets?
def replicate_snaps_incremental(
    source_cli: ZfsCli,
    source_snaps: Collection[Snapshot],
    dest_cli: ZfsCli,
    dest_snaps: Collection[Snapshot],
    rollback: bool,
    log_indent: int = 0
):
    """
    Replicate single source dataset to single dest dataset.

    replicates source_snaps to dest_dataset
    all source_snaps must be of same dataset

    Let S and D be the snapshots on source and dest, newest first.
    Then D[0] = S[b] for some index b.
    We call b the base index. It is used as an incremental basis for sending snapshots S[:b]
    """
    def _s(level: int = 0):
        return space(log_indent + level)

    if not source_snaps:
        log.info(_s() + f'No source snapshots given, nothing to do')
        return

    source_dataset = next(iter(source_snaps)).dataset
    dest_dataset = next(iter(dest_snaps)).dataset

    # Snaps must all be of the same dataset
    assert all(s.dataset == source_dataset for s in source_snaps)
    assert all(s.dataset == dest_dataset for s in dest_snaps)

    # sorting is required
    source_snaps = sort_snaps_by_time(source_snaps, reverse=True)
    dest_snaps = sort_snaps_by_time(dest_snaps, reverse=True)


    ##### PHASE 1: Critical preparation, check for abort conditions

    # resolve hold tags
    source_tag = holdtag_src(dest_cli.get_dataset(dest_dataset))
    dest_tag = holdtag_dest(source_cli.get_dataset(source_dataset))

    # Determine latest common snapshot
    base_snap = determine_latest_common((source_snaps, dest_snaps))

    # Update holds
    ensure_holds(
        (source_cli, dest_cli),
        (source_snaps, dest_snaps),
        (source_tag, dest_tag),
        datasets=(source_dataset, dest_dataset),
        latest_common_snap=base_snap,
        log_indent=log_indent
    )

    if not dest_snaps:
        raise ReplicationError(f"Destination '{dest_dataset}' does not contain any snapshots")

    # figure out base index
    if base_snap is None:
        raise ReplicationError(f"Source '{source_dataset}' and destination '{dest_dataset}' have no common snapshot")
    if base_snap[1].guid != dest_snaps[0].guid:
        raise ReplicationError(f"Destination '{dest_dataset}' has snapshots newer than latest common snapshot '{base_snap[1].shortname}'")
    base_index = next(i for i, s in enumerate(source_snaps) if s.guid == base_snap[0].guid)

    # Ensure base snapshot on dest has correct tags; this may help if previous replication was aborted before tags could be set
    if (_src_tags := base_snap[0].tags) is not None:
        _dest_tags = base_snap[1].tags or set()
        _missing = _src_tags - _dest_tags
        if _missing:
            log.info(_s() + f"Adding missing tags to base snapshot '{base_snap[1].shortname}' on destination")
            dest_cli.set_tags(base_snap[1].longname, _dest_tags | _missing)

    # Determine sequence of source snapshots to transfer.
    # Default: transfer all source snapshots from common base to latest.
    transfer_sequence = list(reversed(source_snaps[:base_index+1]))

    # must at least contain a base snapshot
    assert transfer_sequence

    if len(transfer_sequence) <= 1:
        log.info(_s() + f"Already up to date")
        return

    # Find snapshot that cannot be transferred because their timestamp equals their predecessor
    for i, (a, b) in enumerate(pairwise(transfer_sequence)):
        if a.timestamp == b.timestamp:
            # Snapshot B cannot be sent
            raise ReplicationError(
                f"Cannot transfer snapshots from '{source_dataset}' to '{dest_dataset}': "
                f"snapshot '{b.shortname}' shares timestamp with predecessor '{a.shortname}'"
            )


    ##### PHASE 2: Everything technically good to go, do some quality-of-life checks before actual transfer

    assert len(transfer_sequence) >= 2
    
    # Optionally ensure dest is at snapshot
    if rollback:
        log.info(_s() + f"Rolling back destination to latest snapshot")
        dest_cli.rollback(dest_snaps[0].longname)


    ##### PHASE 3: Transfer snapshots sequentially

    total = len(transfer_sequence) - 1
    log.info(_s() + f"Destination is {total} snapshots behind")
    for i, (_base, _snap) in enumerate(pairwise(transfer_sequence)):
        log.info(_s() + f"Transferring snapshot [{i+1}/{total}]: {_snap.shortname}")
        send_receive_incremental(
            clis=(source_cli, dest_cli),
            dest_dataset=dest_dataset,
            holdtags=(source_tag, dest_tag),
            snapshot=_snap,
            base=_base,  # guaranteed to have hold
            log_indent=log_indent + 1
        )
    dest_snaps = [s.with_dataset(dest_dataset) for s in reversed(transfer_sequence[1:])] + dest_snaps


def ensure_holds(
    clis: tuple[ZfsCli, ZfsCli],
    snaps: tuple[list[Snapshot], list[Snapshot]],
    holdtags: tuple[str, str],
    latest_common_snap: tuple[Snapshot, Snapshot] | None,
    datasets: tuple[Path, Path],
    log_indent: int = 0
):
    """Ensures the latest common snapshot is held on both sides. Removes all other peer holdtags.

    After completion, one of these is true:
    1. There are no holdtags on either side, since there was no common snapshot
    2. There is exactly one holdtag on each side, on the latest common snapshot
    """
    def _s(level: int = 0):
        return space(log_indent + level)

    # Get holds
    holds = (
        clis[0].get_holdtags([s.longname for s in snaps[0]], userrefs={s.longname: s.holds for s in snaps[0]}),
        clis[1].get_holdtags([s.longname for s in snaps[1]], userrefs={s.longname: s.holds for s in snaps[1]})
    )

    if latest_common_snap is None:
        # Remove all peer holdtags
        release_snaps = (
            [s.longname for s in snaps[0]],
            [s.longname for s in snaps[1]]
        )
        _release_holds(clis, release_snaps, holdtags, current_holdtags=holds, datasets=datasets, log_indent=log_indent)
        return

    # Ensure latest common snap is held
    src_snap, dest_snap = latest_common_snap
    if holdtags[0] not in holds[0][src_snap.longname]:
        log.info(_s() + f"Creating hold for latest common snapshot '{src_snap.shortname}' on source")
        clis[0].hold([src_snap.longname], tag=holdtags[0])
    if holdtags[1] not in holds[1][dest_snap.longname]:
        log.info(_s() + f"Creating hold for latest common snapshot '{dest_snap.shortname}' on destination")
        clis[1].hold([dest_snap.longname], tag=holdtags[1])

    # Remove all other holdtags
    release_snaps = (
        [s.longname for s in snaps[0] if s.guid != latest_common_snap[0].guid],
        [s.longname for s in snaps[1] if s.guid != latest_common_snap[1].guid]
    )
    _release_holds(clis, release_snaps, holdtags, current_holdtags=holds, datasets=datasets, log_indent=log_indent)


def determine_latest_common(snaps: tuple[list[Snapshot],list[Snapshot]]) -> tuple[Snapshot, Snapshot] | None:
    """Finds the latest snapshot that exists on both sides."""
    guid_to_snap = (
        {s.guid: s for s in snaps[0]},
        {s.guid: s for s in snaps[1]}
    )
    common_guids = guid_to_snap[0].keys() & guid_to_snap[1].keys()
    if not common_guids:
        return None

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
    snaps_longnames: tuple[list[str], list[str]],
    release_holdtags: tuple[str, str],
    current_holdtags: tuple[dict[str, set[str]], dict[str, set[str]]],
    datasets: tuple[Path, Path],
    log_indent: int = 0
):
    def _s(level: int = 0):
        return space(log_indent + level)

    # Filter for snaps that have the holdtags
    release_snaps = (
        [s for s in snaps_longnames[0] if release_holdtags[0] in current_holdtags[0][s]],
        [s for s in snaps_longnames[1] if release_holdtags[1] in current_holdtags[1][s]],
    )
    if release_snaps[0]:
        log.info(_s() + f"Releasing {len(release_snaps[0])} obsolete holds on source")
    if release_snaps[1]:
        log.info(_s() + f"Releasing {len(release_snaps[1])} obsolete holds on destination")
    clis[0].release_hold(release_snaps[0], release_holdtags[0])
    clis[1].release_hold(release_snaps[1], release_holdtags[1])

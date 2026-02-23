from __future__ import annotations
import logging

from zfsnappr.common.zfs import ZfsCli, Hold, Snapshot
from .args import Args
from zfsnappr.common.filter import SnapFilter
from zfsnappr.common.path import Path
from zfsnappr.common.resolve_datasets import ResolvedDatasets
from zfsnappr.common.command_utils import fetch_snaps, resolve_dataset_args, resolve_filter_args
from zfsnappr.common.utils import space, group_by, sort_dict
from zfsnappr.common.sort import sortkey_dataset, sortkey_snap_by_time


log = logging.getLogger(__name__)


def entrypoint(args: Args) -> None:
    resolved = resolve_dataset_args(args)
    filter = resolve_filter_args(shortnames=args.snapshot)

    _first = True
    for conn, (datasets, cli) in resolved.items():
        if not _first:
            log.info("")
        _first = False

        log.info(f"[{conn}] Scanning snapshot holds on {len(datasets.matched)} datasets")
        unhold_conn(cli=cli, datasets=datasets, filter=filter, dry_run=args.dry_run)


def unhold_conn(cli: ZfsCli, datasets: ResolvedDatasets, filter: SnapFilter, dry_run: bool):
    snaps = fetch_snaps(cli, datasets, filter=filter)

    # Get hold tags and filter
    _all_holds = cli.get_holds([s.longname for s in snaps], userrefs={s.longname: s.holds for s in snaps})
    release_holds = {h for h in _all_holds if h.tag.startswith('zfsnappr')}

    # Print result
    print_result(release_holds, datasets=datasets, snaps=snaps)

    if dry_run:
        log.info(space(1) + f"Dry-run enabled, not releasing any holds")
        return

    # Release holds
    total = len(release_holds)
    log.info(space(1) + f"Releasing {total} holds on {len(set(h.dataset for h in release_holds))} datasets")
    for i, hold in enumerate(release_holds):
        cli.release_hold([hold.snap_longname], tag=hold.tag)
        log.info(space(2) + f"{i+1}/{total} released")


def print_result(release_holds: set[Hold], datasets: ResolvedDatasets, snaps: list[Snapshot]):
    longname_to_snap = {(s.dataset, s.shortname): s for s in snaps}
    dataset_to_holds = sort_dict(
        group_by(release_holds, key=lambda h: h.dataset, ensure_keys=datasets.p.matched),
        key=sortkey_dataset
    )
    for dataset, holds in dataset_to_holds.items():
        log.info(space(1) + f"Dataset: {dataset}")

        if not holds:
            log.info(space(2) + f"No releasable holds")
            continue

        snap_to_holds = sort_dict(
            group_by(holds, lambda h: longname_to_snap[(dataset, h.snap_shortname)]),
            key=sortkey_snap_by_time
        )
        log.info(space(2) + f"Release {len(holds)} holds on {len(snap_to_holds)} snapshots:")
        for snap, _holds in snap_to_holds.items():
            _holds_str = ', '.join(h.tag for h in _holds)
            log.info(space(3) + f"{snap.shortname}: {_holds_str}")

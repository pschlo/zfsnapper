from __future__ import annotations
import logging

from zfsnappr.common.zfs import ZfsCli
from .args import Args
from zfsnappr.common.filter import SnapFilter
from zfsnappr.common.resolve_datasets import ResolvedDatasets
from zfsnappr.common.command_utils import fetch_snaps, resolve_dataset_args, resolve_filter_args


log = logging.getLogger(__name__)


def entrypoint(args: Args) -> None:
    resolved = resolve_dataset_args(args)
    filter = resolve_filter_args(shortnames=args.snapshot)

    _first = True
    for conn, (datasets, cli) in resolved.items():
        if not _first:
            log.info("")
        _first = False

        log.info(f"Location: {conn}")
        unhold_conn(cli=cli, datasets=datasets, filter=filter)


def unhold_conn(cli: ZfsCli, datasets: ResolvedDatasets, filter: SnapFilter):
    snaps = fetch_snaps(cli, datasets, filter=filter)
    if not snaps:
        log.info(f"No matching snapshots, nothing to do")
        return

    # get hold tags
    _all_holds = cli.get_holds([s.longname for s in snaps], userrefs={s.longname: s.holds for s in snaps})
    release_holds = [h for h in _all_holds if h.tag.startswith('zfsnappr')]
    if not release_holds:
        log.info(f"Snapshots have no releasable holds")
        return

    # Release all zfsnappr holds
    for hold in release_holds:
        log.info(f"Releasing hold '{hold.tag}' on snapshot '{hold.snap_longname}'")
        cli.release_hold([hold.snap_longname], tag=hold.tag)

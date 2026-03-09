from __future__ import annotations
from typing import cast, Optional, TYPE_CHECKING
import logging
from collections.abc import Collection

from zfsnapper.common.zfs import ZfsProperty, ZfsCli, Dataset, Snapshot
from zfsnapper.common.resolve_datasets import ResolvedDatasets
from zfsnapper.common.command_utils import fetch_snaps, resolve_dataset_args, resolve_filter_args
from zfsnapper.common.filter import SnapFilter
from zfsnapper.common.parse_dataset_arg import ConnSpec

from .policy import KeepPolicy
from .prune_snaps import prune_snapshots
from .grouping import groupers, Grouper
if TYPE_CHECKING:
    from .args import Args


log = logging.getLogger(__name__)


def entrypoint(args: Args):
    policy = KeepPolicy(
        last = args.keep_last,
        hourly = args.keep_hourly,
        daily = args.keep_daily,
        weekly = args.keep_weekly,
        monthly = args.keep_monthly,
        yearly = args.keep_yearly,

        within = args.keep_within,
        within_hourly = args.keep_within_hourly,
        within_daily = args.keep_within_daily,
        within_weekly = args.keep_within_weekly,
        within_monthly = args.keep_within_monthly,
        within_yearly = args.keep_within_yearly,

        name = args.keep_name,
        tags = frozenset(args.keep_tag)
    )

    # Determine grouper
    grouper: Grouper
    if args.group_by == 'dataset':
        grouper = groupers.DATASET
    elif args.group_by == '':
        grouper = groupers.NOGROUP
    else:
        assert False

    resolved = resolve_dataset_args(args)
    filter = resolve_filter_args(tag_groups=args.tag, shortnames=args.snapshot)

    _first = True
    for conn, (datasets, cli) in resolved.items():
        if not _first:
            log.info("")
        _first = False

        prune_conn(
            cli=cli,
            datasets=datasets,
            conn=conn,
            policy=policy,
            grouper=grouper,
            filter=filter,
            allow_destroy_all=args.allow_destroy_all or bool(args.snapshot),  # only allow if specific snapshots were passed
            dry_run=args.dry_run,
        )


def prune_conn(
    cli: ZfsCli,
    conn: ConnSpec,
    datasets: ResolvedDatasets,
    policy: KeepPolicy,
    grouper: Grouper,
    filter: SnapFilter,
    allow_destroy_all: bool,
    dry_run: bool
):
    # Fetch all snapshots for all datasets
    snaps = fetch_snaps(cli, datasets, filter=filter)
    if not snaps:
        log.info(f"[{conn}] No matching snapshots, nothing to do")
        return

    prune_snapshots(
        cli,
        snaps,
        policy,
        conn=conn,
        dry_run=dry_run,
        grouper=grouper,
        allow_destroy_all=allow_destroy_all
    )

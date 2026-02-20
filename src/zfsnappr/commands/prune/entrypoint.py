from __future__ import annotations
from typing import cast, Optional, TYPE_CHECKING
import logging
from collections.abc import Collection

from zfsnappr.common.zfs import ZfsProperty, ZfsCli, Dataset, Snapshot
from zfsnappr.common.resolve_datasets import ResolvedDatasets
from zfsnappr.common.command_utils import fetch_snaps, resolve_dataset_args, resolve_filter_args
from zfsnappr.common.filter import SnapFilter

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
  grouper: Grouper | None
  if args.group_by == 'dataset':
     grouper = groupers.DATASET
  elif args.group_by == '':
     grouper = None
  else:
     assert False

  resolved = resolve_dataset_args(args)
  filter = resolve_filter_args(tag_groups=args.tag, shortnames=args.snapshot)

  for i, (conn, (datasets, cli)) in enumerate(resolved.items()):
    log.info(f"Location: {conn}")
    prune_conn(
       cli=cli,
       datasets=datasets,
       policy=policy,
       grouper=grouper,
       filter=filter,
       dry_run=args.dry_run,
    )
    if i < len(resolved)-1:
      log.info("")


def prune_conn(
    cli: ZfsCli,
    datasets: ResolvedDatasets,
    policy: KeepPolicy,
    grouper: Grouper | None,
    filter: SnapFilter,
    dry_run: bool
):
    # Fetch all snapshots for all datasets
    snaps = fetch_snaps(cli, datasets, filter=filter)
    if not snaps:
        log.info(f"No matching snapshots, nothing to do")
        return

    prune_snapshots(
        cli,
        snaps,
        policy,
        dry_run=dry_run,
        grouper=grouper,
        allow_destroy_all=filter.shortnames is not None  # only allow if specific snapshots were passed
    )

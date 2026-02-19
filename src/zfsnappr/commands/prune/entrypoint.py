from __future__ import annotations
from typing import cast, Optional, TYPE_CHECKING
import logging
from collections.abc import Collection

from zfsnappr.common.zfs import ZfsProperty, ZfsCli, Dataset, Snapshot
from zfsnappr.common import filter
from zfsnappr.common.resolve_datasets import resolve_dataset_args, ResolvedDatasets
from zfsnappr.common.sort import sort_snaps_by_time
from zfsnappr.common.utils import group_by

from .policy import KeepPolicy
from .prune_snaps import prune_snapshots
from .grouping import GroupType
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

  resolved = resolve_dataset_args(args)
  for i, (conn, (datasets, cli)) in enumerate(resolved.items()):
    log.info(f"Location: {conn}")
    prune_conn(
       cli=cli,
       datasets=datasets,
       policy=policy,
       filter_tags=args.tag,
       filter_snapshots=args.snapshot,
       groupby=args.group_by,
       dry_run=args.dry_run,
    )
    if i < len(resolved)-1:
      log.info("")


def prune_conn(
    cli: ZfsCli,
    datasets: ResolvedDatasets,
    policy: KeepPolicy,
    filter_tags: Collection[str],
    filter_snapshots: Collection[str],
    groupby: str,
    dry_run: bool
):
    # Fetch all snapshots for all datasets
    snaps = [
        *cli.get_all_snapshots([g.name for g in datasets.recursive_groups], recursive=True),
        *cli.get_all_snapshots([d.name for d in datasets.single_datasets])
    ]
    snaps = filter.filter_snaps(snaps, tag=filter.parse_tags(filter_tags), shortname=filter.parse_shortnames(filter_snapshots))
    snaps = sort_snaps_by_time(snaps)
    if not snaps:
        log.info(f"No matching snapshots, nothing to do")
        return

    get_grouptype: dict[str, Optional[GroupType]] = {
        'dataset': GroupType.DATASET,
        '': None
    }

    prune_snapshots(
        cli,
        snaps,
        policy,
        dry_run=dry_run,
        group_by=get_grouptype[groupby],
        allow_destroy_all=bool(filter_snapshots)  # only allow if specific snapshots were passed
    )

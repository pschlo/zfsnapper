from __future__ import annotations
from typing import cast, Optional, TYPE_CHECKING
import logging
from collections.abc import Collection

from zfsnappr.common.zfs import ZfsProperty, ZfsCli, Dataset, Snapshot
from zfsnappr.common.resolve_datasets import ResolvedDatasets
from zfsnappr.common.command_utils import fetch_snaps, resolve_dataset_args

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
       filter_snaps=args.snapshot,
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
    filter_snaps: Collection[str],
    groupby: str,
    dry_run: bool
):
    # Fetch all snapshots for all datasets
    snaps = fetch_snaps(cli, datasets, filter_tags=filter_tags, filter_snaps=filter_snaps)
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
        allow_destroy_all=bool(filter_snaps)  # only allow if specific snapshots were passed
    )

import logging
from collections.abc import Collection

from zfsnappr.common import filter
from zfsnappr.common.args import CommonArgs
from zfsnappr.common.sort import sort_snaps_by_time
from zfsnappr.common.zfs import ZfsCli
from zfsnappr.common.utils import combine_dicts
from zfsnappr.common.resolve_datasets import ResolvedDatasets, resolve_datasets


log = logging.getLogger(__name__)


def resolve_dataset_args(args: CommonArgs):
  """Shorthand function for parsing dataset args."""
  return combine_dicts(
    *resolve_datasets(
        include_exact=args.inc_dataset_exact,
        include_recurse=args.inc_dataset_recurse,
        exclude_exact=args.exc_dataset_exact,
        exclude_recurse=args.exc_dataset_recurse,
        strict=args.strict
    )
  )


def fetch_snaps(
    cli: ZfsCli,
    datasets: ResolvedDatasets,
    props: Collection[str] = [],
    filter_tags: Collection[str] = [],
    filter_snaps: Collection[str] = [],
):
    """Shorthand function for fetching all snapshots of the given `datasets`.
    Snapshots are sorted by creation time, in ascending order.
    """
    snaps = [
        *cli.get_all_snapshots([g.name for g in datasets.recursive_groups], properties=props, recursive=True),
        *cli.get_all_snapshots([d.name for d in datasets.single_datasets], properties=props, recursive=False)
    ]
    snaps = filter.filter_snaps(
        snaps,
        tag=filter.parse_tags(filter_tags),
        shortname=filter.parse_shortnames(filter_snaps)
    )
    snaps = sort_snaps_by_time(snaps)
    return snaps

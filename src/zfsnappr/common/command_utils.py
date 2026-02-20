import logging
from collections.abc import Collection

from zfsnappr.common.filter import SnapFilter, filter_snaps
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


def resolve_filter_args(
    tag_groups: Collection[str] = [],
    shortnames: Collection[str] = []
) -> SnapFilter:
    # Empty tag is preserved; used as token to make it possible to match snapshots without tags.
    tag_groups_resolved = {frozenset(g.split(',')) for g in tag_groups} if tag_groups else None
    shortnames_resolved = set(shortnames) if shortnames else None

    return SnapFilter(
        tag_groups=tag_groups_resolved,
        shortnames=shortnames_resolved,
    )


def fetch_snaps(
    cli: ZfsCli,
    datasets: ResolvedDatasets,
    props: Collection[str] = [],
    filter: SnapFilter = SnapFilter()
):
    """Fetch all snapshots of the given `datasets`.

    Snapshots are sorted by creation time (ascending order) and optionally filtered.
    """
    snaps = [
        *cli.get_all_snapshots([d.path for d in datasets.recursive_groups], properties=props, recursive=True),
        *cli.get_all_snapshots([d.path for d in datasets.single_datasets], properties=props, recursive=False)
    ]
    snaps = filter_snaps(snaps, filter)
    snaps = sort_snaps_by_time(snaps)
    return snaps

import logging
from collections.abc import Collection

from zfsnappr.common.filter import SnapFilter, snapfilters
from zfsnappr.common.args import CommonArgs
from zfsnappr.common.sort import sort_snaps_by_time
from zfsnappr.common.zfs import ZfsCli
from zfsnappr.common.utils import combine_dicts
from zfsnappr.common.resolve_datasets import ResolvedDatasets, resolve_dataset_specs
from zfsnappr.common.parse_dataset_arg import parse_dataset_arg


log = logging.getLogger(__name__)


def resolve_dataset_args(args: CommonArgs):
    """Shorthand function for parsing dataset args."""
    def _parse(raw_specs: Collection[str]):
        return [parse_dataset_arg(s) for s in raw_specs]

    return combine_dicts(
        *resolve_dataset_specs(
            include_exact=_parse(args.inc_dataset_exact),
            include_recurse=_parse(args.inc_dataset_recurse),
            exclude_exact=_parse(args.exc_dataset_exact),
            exclude_recurse=_parse(args.exc_dataset_recurse),
            strict=args.strict
        )
    )


def resolve_filter_args(
    tag_groups: Collection[str] = [],
    shortnames: Collection[str] = []
) -> SnapFilter:
    filter: SnapFilter = snapfilters.Composite()
    if tag_groups:
        # Empty tag is preserved; used as token to make it possible to match snapshots without tags.
        filter &= snapfilters.Tag([g.split(',') for g in tag_groups])
    if shortnames:
        filter &= snapfilters.Shortname(shortnames)
    return filter


def fetch_snaps(
    cli: ZfsCli,
    datasets: ResolvedDatasets,
    props: Collection[str] = [],
    filter: SnapFilter = snapfilters.ALLOW_ALL
):
    """Fetch all snapshots of the given `datasets`.

    Snapshots are sorted by creation time (ascending order) and optionally filtered.
    """
    snaps = [
        *cli.get_all_snapshots([d.path for d in datasets.recursive_root_datasets], properties=props, recursive=True),
        *cli.get_all_snapshots([d.path for d in datasets.explicit_datasets], properties=props, recursive=False)
    ]
    snaps = filter.apply(snaps)
    snaps = sort_snaps_by_time(snaps)
    return snaps

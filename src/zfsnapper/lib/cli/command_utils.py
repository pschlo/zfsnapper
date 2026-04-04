import logging
from collections.abc import Collection

from zfsnapper.common.args import CommonArgs
from zfsnapper.lib.zfs import ZfsCli

from zfsnapper.lib.cli.snapfilter import SnapFilter, snapfilters
from zfsnapper.lib.cli.sortkey import sortkey_snap_by_time
from zfsnapper.lib.cli.utils import combine_dicts
from zfsnapper.lib.cli.resolve_datasets import ResolvedDatasets, resolve_dataset_specs
from zfsnapper.lib.cli.parse_dataset_arg import parse_dataset_arg


log = logging.getLogger(__name__)


def resolve_dataset_args(
    args: CommonArgs | None = None,
    *,

    # Override args
    include_exact: Collection[str] = [],
    include_recurse: Collection[str] = [],
    exclude_exact: Collection[str] = [],
    exclude_recurse: Collection[str] = [],
    strict: bool | None = None,

    # Other args
    default_all_local: bool = False  # If no datasets, default to all datasets
):
    """Shorthand function for parsing dataset args."""
    def _parse(raw_specs: Collection[str]):
        return [parse_dataset_arg(s) for s in raw_specs]

    return combine_dicts(
        *resolve_dataset_specs(
            include_exact=_parse(include_exact or (args.inc_dataset_exact if args else [])),
            include_recurse=_parse(include_recurse or (args.inc_dataset_recurse if args else [])),
            exclude_exact=_parse(exclude_exact or (args.exc_dataset_exact if args else [])),
            exclude_recurse=_parse(exclude_recurse or (args.exc_dataset_recurse if args else [])),
            strict=strict if strict is not None else (args.strict if args else False),
            default_all_local=default_all_local
        )
    )


def resolve_filter_args(
    match_tag_groups: Collection[str] = [],
    match_shortnames: Collection[str] = [],
    exclude_tag_groups: Collection[str] = []
) -> SnapFilter:
    filter: SnapFilter = snapfilters.Composite()
    if match_tag_groups:
        # Empty tag is preserved; used as token to make it possible to match snapshots without tags.
        filter &= snapfilters.MatchTag([g.split(',') for g in match_tag_groups])
    if exclude_tag_groups:
        # Empty tag is preserved; used as token to make it possible to exclude snapshots without tags.
        filter &= snapfilters.ExcludeTag([g.split(',') for g in exclude_tag_groups])
    if match_shortnames:
        filter &= snapfilters.MatchShortname(match_shortnames)
    return filter


def fetch_snaps(
    cli: ZfsCli,
    datasets: ResolvedDatasets,
    props: Collection[str] = [],
    filter: SnapFilter = snapfilters.ALLOW_ALL,
    ignore_holdtags: bool = False
):
    """Fetch all snapshots of the given `datasets`.

    Snapshots are sorted by creation time (ascending order) and optionally filtered.
    """
    snaps = cli.get_snapshots(datasets.p.matched, properties=props, holdtags=not ignore_holdtags)
    snaps = filter.apply(snaps)
    snaps = sorted(snaps, key=sortkey_snap_by_time)
    return snaps

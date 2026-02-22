from __future__ import annotations
import logging

from zfsnappr.common.replication import replicate, replicate_snaps
from zfsnappr.common.resolve_datasets import ResolvedDatasets, create_zfs_cli, resolve_datasets
from zfsnappr.common.command_utils import resolve_dataset_args, fetch_snaps
from zfsnappr.common.parse_dataset_arg import parse_dataset_arg
from zfsnappr.common.path import Path, longest_common_ancestor
from zfsnappr.common.zfs import ZfsCli
from zfsnappr.common.utils import group_by, combine_dicts
from .args import Args


log = logging.getLogger(__name__)


def entrypoint(args: Args) -> None:
    src_resolved = resolve_dataset_args(args)
    dest_spec = parse_dataset_arg(args.dest)
    all_dest_datasets = ...
    dest_cli = create_zfs_cli(dest_spec.conn)

    for conn, (datasets, cli) in src_resolved.items():
        push_conn(source_cli=cli, datasets=datasets, dest_cli=dest_cli, dest_root=dest_spec.dataset)


def push_conn(source_cli: ZfsCli, datasets: ResolvedDatasets, dest_root: Path, dest_cli: ZfsCli):
    # Push MULTIPLE source datasets to SINGLE dest dataset
    # Find longest common prefix of source datasets and take this as src root
    # Map src root to dst root

    # Find longest common src prefix; may be empty path
    src_root = longest_common_ancestor([d.path for d in datasets.kept_datasets])

    print(src_root)

    # Create matching of source dataset to dest dataset
    srcpath_to_destpath = {
        ds.path: dest_root / ds.path.relative_to(src_root)
        for ds in datasets.kept_datasets
    }

    # Fetch all source snapshots
    srcpath_to_snaps = group_by(
        fetch_snaps(cli=source_cli, datasets=datasets),
        lambda s: s.dataset
    )

    # Fetch all dest snapshots
    _dest_datasets = resolve_datasets(
        include_exact=srcpath_to_destpath.values()
    )
    destpath_to_snaps = group_by(
        fetch_snaps(cli=dest_cli, datasets=...),
        lambda s: s.dataset
    )

    # Replicate dataset-by-dataset
    for src_dataset, (dest_dataset, src_snaps) in combine_dicts(srcpath_to_destpath, srcpath_to_snaps).items():
        replicate_snaps()

    # prefix = "Recursively pushing" if args.recursive else "Pushing"
    # log.info(prefix + f' from source "{source_dataset}" to dest "{dest_dataset}"')

    # replicate(
    #     source_cli=source_cli,
    #     source_dataset=source_dataset,
    #     dest_cli=dest_cli,
    #     dest_dataset=dest_root,
    #     recursive=args.recursive,
    #     initialize=args.init,
    #     rollback=args.rollback,
    #     exclude_datasets=args.exclude_dataset
    # )

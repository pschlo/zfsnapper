from __future__ import annotations
import logging

from zfsnappr.common.replication import ReplicationError, replicate_snaps_initial, replicate_snaps_incremental
from zfsnappr.common.resolve_datasets import ResolvedDatasets, create_zfs_cli, resolve_conn_datasets
from zfsnappr.common.command_utils import resolve_dataset_args, fetch_snaps
from zfsnappr.common.parse_dataset_arg import parse_dataset_arg, DatasetSpec, ConnSpec
from zfsnappr.common.path import Path, longest_common_ancestor
from zfsnappr.common.zfs import ZfsCli
from zfsnappr.common.utils import group_by, combine_dicts
from .args import Args


log = logging.getLogger(__name__)


def entrypoint(args: Args) -> None:
    src_resolved = resolve_dataset_args(args)
    dest_spec = parse_dataset_arg(args.dest)
    dest_cli = create_zfs_cli(dest_spec.conn)

    # _first = True
    for conn, (datasets, cli) in src_resolved.items():
        # if not _first:
        #     log.info("")
        # _first = False

        # log.info(f"Location: {conn}")
        push_conn(
            src_cli=cli,
            src_datasets=datasets,
            dest_cli=dest_cli,
            dest_root=dest_spec.dataset,
            allow_initialize=args.init,
            rollback=args.rollback,
            src_conn=conn,
            dst_conn=dest_spec.conn
        )


def push_conn(src_cli: ZfsCli, src_datasets: ResolvedDatasets, dest_root: Path, dest_cli: ZfsCli, allow_initialize: bool, rollback: bool, src_conn: ConnSpec, dst_conn: ConnSpec):
    # Push MULTIPLE source datasets to SINGLE dest dataset
    # Find longest common prefix of source datasets and take this as src root
    # Map src root to dst root

    # Find longest common src prefix; may be empty path
    src_root = longest_common_ancestor([d.path for d in src_datasets.matching_datasets])
    log.info(f"Replicating from source root '{src_conn}/{src_root}' to destination root '{dst_conn}/{dest_root}'")

    # Create matching of source dataset to dest dataset
    srcpath_to_destpath = {
        src_path: dest_root / src_path.relative_to(src_root)
        for src_path in sorted(src_datasets.matching_paths, key=lambda p: p.depth)
    }

    # Determine corresponding dest datasets
    # Some expected dest datasets may be missing
    dest_datasets = resolve_conn_datasets(
        cli=dest_cli,
        include_exact=srcpath_to_destpath.values()
    )

    # Determine missing dest datasets
    missing_dest_paths = set(srcpath_to_destpath.values()) - dest_datasets.matching_paths

    # Fetch all snapshots.
    srcpath_to_snaps = group_by(
        fetch_snaps(cli=src_cli, datasets=src_datasets),
        lambda s: s.dataset,
        ensure_keys=src_datasets.matching_paths
    )
    destpath_to_snaps = group_by(
        fetch_snaps(cli=dest_cli, datasets=dest_datasets),
        lambda s: s.dataset,
        ensure_keys=dest_datasets.matching_paths
    )

    # Replicate dataset-by-dataset
    _first = True
    for srcpath, destpath in srcpath_to_destpath.items():
        if not _first:
            log.info("")
        _first = False

        log.info(f"Transferring from '{src_conn}/{srcpath}' to '{dst_conn}/{destpath}'")
        src_snaps = srcpath_to_snaps[srcpath]

        if destpath in missing_dest_paths:
            # Dest dataset does not exist; cannot fetch snapshots.
            if not allow_initialize:
                raise ReplicationError(f"Destination dataset '{destpath}' does not exist and will not be created")
            # Do initial send-receive to create dest dataset.
            replicate_snaps_initial(
                source_cli=src_cli,
                source_dataset=src_datasets.path_to_dataset[srcpath],
                source_snaps=src_snaps,
                dest_dataset=destpath,
                dest_cli=dest_cli
            )
        else:
            # Do incremental send-receive.
            dest_snaps = destpath_to_snaps[destpath]
            replicate_snaps_incremental(
                source_cli=src_cli,
                source_snaps=src_snaps,
                dest_cli=dest_cli,
                dest_snaps=dest_snaps,
                rollback=rollback
            )

        log.info(f"Transfer from '{src_conn}/{srcpath}' to '{dst_conn}/{destpath}' complete")


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

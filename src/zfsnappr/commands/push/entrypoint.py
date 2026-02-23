from __future__ import annotations
import logging

from zfsnappr.common.replication import ReplicationError, replicate_snaps_initial, replicate_snaps_incremental
from zfsnappr.common.resolve_datasets import ResolvedDatasets, create_zfs_cli, resolve_conn_datasets
from zfsnappr.common.command_utils import resolve_dataset_args, fetch_snaps
from zfsnappr.common.parse_dataset_arg import parse_dataset_arg, DatasetSpec, ConnSpec
from zfsnappr.common.path import Path
from zfsnappr.common.zfs import ZfsCli
from zfsnappr.common.utils import group_by, combine_dicts, space
from .args import Args


log = logging.getLogger(__name__)


def entrypoint(args: Args) -> None:
    src_resolved = resolve_dataset_args(args)
    dest_spec = parse_dataset_arg(args.dest)
    dest_cli = create_zfs_cli(dest_spec.conn)

    _first = True
    for conn, (datasets, cli) in src_resolved.items():
        if not _first:
            log.info("")
        _first = False

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


def push_conn(
    src_cli: ZfsCli,
    dest_cli: ZfsCli,
    src_datasets: ResolvedDatasets,
    dest_root: Path,
    allow_initialize: bool,
    rollback: bool,
    src_conn: ConnSpec,
    dst_conn: ConnSpec
):
    """
    Push MULTIPLE source datasets to SINGLE dest dataset
    """
    def _s(level: int = 0):
        return space(level)

    # Find longest common src prefix; may be empty path
    src_root = src_datasets.p.deepest_common_ancestor
    log.info(f"Replicating: {src_conn}/{src_root} → {dst_conn}/{dest_root}")

    # Create matching of source dataset to dest dataset
    srcpath_to_destpath = {
        src_path: dest_root / src_path.relative_to(src_root)
        for src_path in sorted(src_datasets.p.paths, key=lambda p: p.depth)
    }

    # Determine corresponding dest datasets
    # Some expected dest datasets may be missing
    dest_datasets = resolve_conn_datasets(
        cli=dest_cli,
        include_exact=srcpath_to_destpath.values()
    )

    # Determine missing dest datasets
    missing_dest_paths = set(srcpath_to_destpath.values()) - dest_datasets.p.paths

    # Fetch all snapshots.
    srcpath_to_snaps = group_by(
        fetch_snaps(cli=src_cli, datasets=src_datasets),
        lambda s: s.dataset,
        ensure_keys=src_datasets.p.paths
    )
    destpath_to_snaps = group_by(
        fetch_snaps(cli=dest_cli, datasets=dest_datasets),
        lambda s: s.dataset,
        ensure_keys=dest_datasets.p.paths
    )

    # Replicate dataset-by-dataset
    is_error = False
    for srcpath, destpath in srcpath_to_destpath.items():
        try:
            relpath = srcpath.relative_to(src_root)
            log.info(_s(1) + f"Checking dataset: ~{f'/{relpath}' if relpath else ''}")
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
                    dest_cli=dest_cli,
                    dest_root=dest_root,
                    log_indent=2
                )
            else:
                # Do incremental send-receive.
                dest_snaps = destpath_to_snaps[destpath]
                replicate_snaps_incremental(
                    source_cli=src_cli,
                    source_snaps=src_snaps,
                    dest_cli=dest_cli,
                    dest_snaps=dest_snaps,
                    rollback=rollback,
                    log_indent=2
                )
        except ReplicationError as e:
            is_error = True
            log.error(e)

    if is_error:
        raise ReplicationError(f"Replication failed for one or more datasets")

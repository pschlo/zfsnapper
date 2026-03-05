from __future__ import annotations
import logging

from zfsnappr.common.replication import ReplicationError
from zfsnappr.common.resolve_datasets import ResolvedDatasets, create_zfs_cli, resolve_conn_datasets
from zfsnappr.common.command_utils import resolve_dataset_args, fetch_snaps
from zfsnappr.common.parse_dataset_arg import parse_dataset_arg, ConnSpec
from zfsnappr.common.sort import sortkey_dataset
from zfsnappr.common.path import Path
from zfsnappr.common.zfs import ZfsCli, Pool
from zfsnappr.common.utils import group_by, space
from .replicate import replicate, DatasetSide, NOT_SET
from .args import Args


log = logging.getLogger(__name__)


def entrypoint(args: Args) -> None:
    src_resolved = resolve_dataset_args(args)
    dest_spec = parse_dataset_arg(args.dest)
    if not dest_spec.dataset:
        raise ValueError("No destination dataset root specified")
    dest_cli = create_zfs_cli(dest_spec.conn)

    # Determine dest pool GUID
    dest_dataset_poolname = dest_spec.dataset[0]
    dest_pool = dest_cli.get_pool(dest_dataset_poolname)

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
            dest_pool=dest_pool,
            allow_init=args.init,
            rollback=args.rollback,
            src_conn=conn,
            dst_conn=dest_spec.conn
        )


def push_conn(
    src_cli: ZfsCli,
    dest_cli: ZfsCli,
    src_datasets: ResolvedDatasets,
    dest_root: Path,
    dest_pool: Pool,
    allow_init: bool,
    rollback: bool,
    src_conn: ConnSpec,
    dst_conn: ConnSpec
):
    """
    Push MULTIPLE source datasets to SINGLE dest dataset
    """
    def _s(level: int = 0):
        return space(level)
    
    # Identify src pools
    _poolnames = {d.poolname for d in src_datasets.matched}
    src_pools = {pool.name: pool for pool in src_cli.get_pools(_poolnames)}
    

    # Find longest common src prefix; may be empty path
    src_root = src_datasets.p.deepest_common_ancestor
    log.info(f"[{src_conn} → {dst_conn}] Replicating: {src_root}/* → {dest_root}/*")

    # Create matching of source dataset to dest dataset
    srcpath_to_destpath = {
        src_path: dest_root / src_path.relative_to(src_root)
        for src_path in sorted(src_datasets.p.matched, key=sortkey_dataset)
    }

    # Determine corresponding dest datasets
    # Some expected dest datasets may be missing
    dest_datasets = resolve_conn_datasets(
        cli=dest_cli,
        include_exact=srcpath_to_destpath.values()
    )

    # Determine missing dest datasets
    missing_dest_paths = set(srcpath_to_destpath.values()) - dest_datasets.p.matched

    # Fetch all snapshots.
    srcpath_to_snaps = group_by(
        fetch_snaps(cli=src_cli, datasets=src_datasets),
        lambda s: s.dataset,
        ensure_keys=src_datasets.p.matched
    )
    destpath_to_snaps = group_by(
        fetch_snaps(cli=dest_cli, datasets=dest_datasets),
        lambda s: s.dataset,
        ensure_keys=dest_datasets.p.matched
    )

    # Replicate dataset-by-dataset
    is_error = False
    for srcpath, destpath in srcpath_to_destpath.items():
        relpath = srcpath.relative_to(src_root)
        source = DatasetSide(
            conn=src_conn,
            cli=src_cli,
            root=src_root,
            path=srcpath,
            pool=src_pools[srcpath[0]],
            dataset=src_datasets.path_to_dataset[srcpath],
            snaps=srcpath_to_snaps[srcpath]
        )
        dest = DatasetSide(
            conn=dst_conn,
            cli=dest_cli,
            root=dest_root,
            pool=dest_pool,
            path=destpath,
            dataset=dest_datasets.path_to_dataset[destpath] if destpath not in missing_dest_paths else NOT_SET,
            snaps=destpath_to_snaps[destpath] if destpath not in missing_dest_paths else NOT_SET
        )
        try:
            log.info(_s(1) + f"Checking dataset: ~{f'/{relpath}' if relpath else ''}")
            replicate(source, dest, relpath=relpath, rollback=rollback, allow_init=allow_init, log_indent=2)
        except ReplicationError as e:
            is_error = True
            log.error(space(e.log_indent) + str(e))

    if is_error:
        raise ReplicationError(f"Replication failed for one or more datasets")

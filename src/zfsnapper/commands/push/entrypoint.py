from __future__ import annotations
from typing import Literal, TYPE_CHECKING
import logging

from zfsnapper.common.replication import ReplicationError
from zfsnapper.common.resolve_datasets import ResolvedDatasets, create_zfs_cli, resolve_conn_datasets
from zfsnapper.common.command_utils import resolve_dataset_args, fetch_snaps
from zfsnapper.common.parse_dataset_arg import parse_dataset_arg, ConnSpec
from zfsnapper.common.sort import sortkey_dataset
from zfsnapper.common.path import Path
from zfsnapper.common.zfs import ZfsCli, Pool
from zfsnapper.common.utils import group_by, space
from .replicate import replicate, DatasetSide, NOT_SET, EncryptionMode

if TYPE_CHECKING:
    from .args import Args


log = logging.getLogger(__name__)


def entrypoint(args: Args) -> None:
    assert args.enc_mode in (EncryptionMode.KEEP, EncryptionMode.CLEAR)

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
            dst_conn=dest_spec.conn,
            enc_mode=args.enc_mode,
            localhost=args.localhost
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
    dst_conn: ConnSpec,
    enc_mode: EncryptionMode,
    localhost: str | None
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
    log.info(f"[{src_conn.format(localhost)} 🡒 {dst_conn.format(localhost)}] Replicating: {src_root}/* 🡒 {dest_root}/*  ({enc_mode.value.lower()}ing source encryption)")

    # Create matching of source dataset to dest dataset
    relpath_to_paths: dict[Path, tuple[Path, Path]] = {
        src_path.relative_to(src_root): (src_path, dest_root / src_path.relative_to(src_root))
        for src_path in sorted(src_datasets.p.matched, key=sortkey_dataset)
    }
    _expected_dest_paths = [p[1] for p in relpath_to_paths.values()]

    # Determine corresponding dest datasets
    # Some expected dest datasets may be missing
    dest_datasets = resolve_conn_datasets(
        cli=dest_cli,
        include_exact=_expected_dest_paths
    )

    # Determine missing dest datasets
    missing_dest_paths = set(_expected_dest_paths) - dest_datasets.p.matched

    # Create closure
    def _create_pairs(skip_relpaths: set[Path] = set()):
        # Filter relpaths
        _relpath_to_paths = {
            relpath: paths
            for relpath, paths in relpath_to_paths.items()
            if relpath not in skip_relpaths
        }

        return create_pairs(
            src_cli=src_cli,
            dest_cli=dest_cli,
            src_datasets=src_datasets,
            dest_datasets=dest_datasets,
            dest_root=dest_root,
            src_root=src_root,
            src_pools=src_pools,
            dest_pool=dest_pool,
            src_conn=src_conn,
            dst_conn=dst_conn,

            relpath_to_paths=_relpath_to_paths,
            missing_dest_paths=missing_dest_paths
        )

    pairs = _create_pairs()
    completed_relpaths: set[Path] = set()
    for relpath in relpath_to_paths.keys():
        _consecutive_fails: int = 0

        while True:
            source, dest = pairs[relpath]

            try:
                log.info(_s(1) + f"Checking dataset: ~{f'/{relpath}' if relpath else ''}")
                replicate(
                    source,
                    dest,
                    relpath=relpath,
                    rollback=rollback,
                    allow_init=allow_init,
                    enc_mode=enc_mode,
                    localhost=localhost,
                    log_indent=2
                )
                completed_relpaths.add(relpath)
                break

            except ReplicationError as e:
                log.error(space(e.log_indent) + str(e))

                if e.snaps_sent:
                    # We made some progress
                    _consecutive_fails = 0
                else:
                    # We did not make any progress
                    _consecutive_fails += 1

                if _consecutive_fails >= 3:
                    # We did not make any progress three times in a row; give up
                    log.error(_s(2) + f"Replication failed three times in a row without making progress; giving up")
                    raise ReplicationError(f"Replication failed for dataset: {source.path}")

                # Keep trying; refetch all snapshots and retry this dataset
                log.info(_s(2) + f"Retrying")
                pairs = _create_pairs(skip_relpaths=completed_relpaths)
                continue

            assert False


def create_pairs(
    src_cli: ZfsCli,
    dest_cli: ZfsCli,
    src_datasets: ResolvedDatasets,
    dest_datasets: ResolvedDatasets,
    src_root: Path,
    dest_root: Path,
    src_pools: dict[str, Pool],
    dest_pool: Pool,
    src_conn: ConnSpec,
    dst_conn: ConnSpec,
    relpath_to_paths: dict[Path, tuple[Path, Path]],
    missing_dest_paths: set[Path]
) -> dict[Path, tuple[DatasetSide, DatasetSide]]:
    """Fetch source + dest snapshots and create dataset sides.

    src_datasets.matched == [p[1].dataset for p in pairs]
    """
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

    # Create dataset sides
    sides: dict[Path, tuple[DatasetSide, DatasetSide]] = {}
    for relpath, (srcpath, destpath) in relpath_to_paths.items():
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
        sides[relpath] = (source, dest)

    return sides

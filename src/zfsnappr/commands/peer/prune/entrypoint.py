from __future__ import annotations
import logging
from dateutil.relativedelta import relativedelta
from datetime import datetime

from .args import Args
from zfsnappr.common.zfs import ZfsCli, PeerInfo, Dataset, Pool
from zfsnappr.common.command_utils import fetch_snaps, resolve_dataset_args, remove_peer, get_holds, parse_holdtags, get_peerinfo
from zfsnappr.common.resolve_datasets import resolve_dataset_specs, combine_dicts
from zfsnappr.common.parse_dataset_arg import parse_dataset_arg
from zfsnappr.common.parse_dataset_arg import ConnSpec, DatasetSpec, Path
from zfsnappr.common.sort import sortkey_dataset
from zfsnappr.common.utils import group_by, space
from zfsnappr.common.resolve_datasets import ResolvedDatasets
from zfsnappr.common.parse_duration import parse_duration

from ..common.get_peers import get_peers


log = logging.getLogger(__name__)


def entrypoint(args: Args) -> None:
    if not any([
        args.peer,
        args.from_,
        args.unused_for,
        args.unheld,
        args.unknown,
        args.all
    ]):
        log.info(f"No prune policy specified, nothing to do")
        return

    resolved = resolve_dataset_args(args)

    prune_exact: set[DatasetSpec] = set()
    for peer in args.peer:
        spec = parse_dataset_arg(peer)
        prune_exact.add(spec)

    sync_conns: set[ConnSpec] = set()
    sync_poolnames: set[tuple[ConnSpec, str]] = set()
    for from_ in args.from_:
        spec = parse_dataset_arg(from_)
        if not spec.dataset:
            sync_conns.add(spec.conn)
        elif len(spec.dataset) == 1:
            sync_poolnames.add((spec.conn, spec.dataset[0]))
        else:
            raise ValueError(f"Peer specification must target either host or pool")

    # Determine which peers to remove.
    # For prune_exact, we already know and don't need to do anything.
    # For sync_conns and sync_pools, we need to fetch existing datasets.

    # Sync conns
    _dest_specs = {
        *(DatasetSpec(conn, Path()) for conn in sync_conns),
        *(DatasetSpec(conn, Path(pool)) for conn, pool in sync_poolnames)
    }
    dest_datasets = combine_dicts(*resolve_dataset_specs(include_recurse=_dest_specs)) if _dest_specs else {}


    # ---- Resolve dest peer GUIDs ----

    sync_conns_guids: dict[ConnSpec, set[int]] = {}
    sync_pools_guids: dict[tuple[ConnSpec, Pool], set[int]] = {}

    for conn, (datasets, cli) in dest_datasets.items():
        if conn in sync_conns:
            # Register all dataset guids
            sync_conns_guids[conn] = {ds.guid for ds in datasets.matched}

        # group datasets by pool
        poolname_to_datasets = group_by(datasets.matched, key=lambda ds: ds.poolname)
        pools = {p.name: p for p in cli.get_pools(poolname_to_datasets.keys())}
        for poolname, _datasets in poolname_to_datasets.items():
            if (conn, poolname) in sync_poolnames:
                sync_pools_guids[(conn, pools[poolname])] = {ds.guid for ds in _datasets}


    _first = True
    for conn, (datasets, cli) in resolved.items():
        if not _first:
            log.info("")
        _first = False

        log.info(f"[{conn}] Pruning peers")
        sync_peer_conn(
            conn=conn,
            cli=cli,
            datasets=datasets,
            prune_exact=prune_exact,
            sync_conns_guids=sync_conns_guids,
            sync_pools_guids=sync_pools_guids,
            dry_run=args.dry_run,
            remove_older_than=parse_duration(args.unused_for) if args.unused_for is not None else None,
            remove_without_holds=args.unheld,
            remove_unknown=args.unknown,
            remove_all=args.all
        )


def sync_peer_conn(
    conn: ConnSpec,
    cli: ZfsCli,
    datasets: ResolvedDatasets,
    prune_exact: set[DatasetSpec],
    sync_conns_guids: dict[ConnSpec, set[int]],
    sync_pools_guids: dict[tuple[ConnSpec, Pool], set[int]],
    dry_run: bool,
    remove_older_than: relativedelta | None,
    remove_without_holds: bool,
    remove_unknown: bool,
    remove_all: bool
):
    """
    - Check existing GUIDs on dest
    - Remove own peers
    - Remove holdtags on snapshots of those peers -> must get holdtags for all
    """
    def _s(i: int = 0):
        return space(i+1)

    snaps = fetch_snaps(cli, datasets)
    holds = get_holds(cli, snaps)
    ds_to_peers, ds_peer_to_holds = get_peers(snaps, holds, datasets)

    now = datetime.now()

    def should_remove(ds: Dataset, peer_guid: int) -> bool:
        if remove_all:
            return True

        p = get_peerinfo(ds, peer_guid)
        if p is None:
            return remove_unknown

        # Check prune_exact
        if DatasetSpec(p.host, p.path) in prune_exact:
            return True

        # Check sync_conns
        for peer_conn, peer_guids in sync_conns_guids.items():
            if p.host == peer_conn and p.guid not in peer_guids:
                return True

        # Check sync_pools
        for (peer_conn, peer_pool), peer_guids in sync_pools_guids.items():
            if p.pool_guid == peer_pool.guid and p.guid not in peer_guids:
                return True

        # Check last used
        if remove_older_than is not None and p.last_used < now - remove_older_than:
            return True

        # Check if no holds
        if remove_without_holds and not ds_peer_to_holds[(ds.path, p.guid)]:
            return True

        return False

    # We don't remove a peer in general; we always remove a peer *from a dataset*.
    # E.g. a peer may be kept on one dataset but removed from another.
    remove_peers: set[tuple[Dataset, int]] = set()
    for d, ps in ds_to_peers.items():
        for p in ps:
            if should_remove(d, p):
                remove_peers.add((d, p))

    if not remove_peers:
        log.info(_s() + f"No peers to remove")
        return

    log.info(_s() + f"Found {len(remove_peers)} peers to remove:")
    for ds, peer in sorted(remove_peers, key=lambda t: sortkey_dataset(t[0])):
        if p := get_peerinfo(ds, peer):
            log.info(_s(1) + f"Peer {p.host}::{p.path} on dataset {ds.path}")
        else:
            log.info(_s(1) + f"Unknown peer on dataset {ds.path}")

    if dry_run:
        log.info(_s() + "Dry-run enabled, not removing any peers")
        return

    log.info(_s() + f"Removing peers")
    for i, (ds, peer) in enumerate(remove_peers):
        remove_peer(cli=cli, dataset=ds, peer_guid=peer, holds=holds, log_indent=2)
        log.info(_s(1) + f"{i+1}/{len(remove_peers)} removed")

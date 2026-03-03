from __future__ import annotations
import logging

from .args import Args
from zfsnappr.common.zfs import ZfsCli, PeerInfo, Dataset
from zfsnappr.common.command_utils import fetch_snaps, resolve_dataset_args, remove_peer, get_holds
from zfsnappr.common.parse_dataset_arg import ConnSpec
from zfsnappr.common.resolve_datasets import ResolvedDatasets


log = logging.getLogger(__name__)


def entrypoint(args: Args) -> None:
    """TODO: Also allow for matching by pool GUID (more precise)"""
    resolved = resolve_dataset_args(args)
    resolved_dests = resolve_dataset_args(include_recurse=args.peer)

    # Dest must contain ALL datasets, otherwise we risk removing peers on source that actually exist
    if conn := next(
        iter(conn for conn, (dataset, cli) in resolved_dests.items() if not dataset.p.is_all_matched),
        None
    ):
        raise ValueError(f"Peer location does not include all datasets: {conn}")

    peer_conn_guids = {conn: {p.guid for p in datasets.matched} for conn, (datasets, _) in resolved_dests.items()}

    # For each dataset, get all snapshots non-recursively
    _first = True
    for conn, (datasets, cli) in resolved.items():
        if not _first:
            log.info("")
        _first = False

        log.info(f"[{conn}] Syncing peers")
        sync_peer_conn(conn=conn, cli=cli, datasets=datasets, peer_conn_guids=peer_conn_guids, dry_run=args.dry_run)


def sync_peer_conn(conn: ConnSpec, cli: ZfsCli, datasets: ResolvedDatasets, peer_conn_guids: dict[ConnSpec, set[int]], dry_run: bool):
    """
    - Check existing GUIDs on dest
    - Remove own peers
    - Remove holdtags on snapshots of those peers -> must get holdtags for all
    """
    # GUID -> [dataset, peerinfo]
    obsolete_peers: dict[int, set[tuple[Dataset, PeerInfo]]] = {}
    for peer_conn, peer_guids in peer_conn_guids.items():
        # Check peer GUIDs and prune source
        expected_peers: dict[int, set[tuple[Dataset, PeerInfo]]] = {}
        for ds in datasets.matched:
            for p in ds.peerinfos.values():
                # Decide whether the PeerInfo belongs to the given peer_conn
                if p and p.host == peer_conn:
                    expected_peers.setdefault(p.guid, set()).add((ds, p))

        # If a GUID were already in obsolete_peers, then our match-peerinfo-to-peerconn would match
        # a single peerinfo to multiple peerconns. This cannot happen, but we stay safe anyway.
        for guid, v in expected_peers.items():
            assert guid not in obsolete_peers
            if guid not in peer_guids:
                obsolete_peers.setdefault(guid, set()).update(v)

    if not obsolete_peers:
        log.info(f"No obsolete peers")
        return

    print(f"Found {len(obsolete_peers)} obsolete peers")
    if dry_run:
        log.info("Dry-run enabled, not removing any peers")
        return

    snaps = fetch_snaps(cli, datasets)
    holds = get_holds(cli, snaps)
    for guid, _datasets in obsolete_peers.items():
        for ds, peer in _datasets:
            remove_peer(cli=cli, dataset=ds, peer_guid=guid, holds=holds)
            print(f"Removed peer {peer.host}::{peer.path} on dataset {ds.path}")


def prune_unused_peers():
    """
    - Filter peers for age
    - Remove peers + holdtags
    """

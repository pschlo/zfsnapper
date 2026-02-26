from __future__ import annotations
from typing import Optional, Callable, cast
from dataclasses import dataclass
from collections.abc import Collection
import logging

from .args import Args
from zfsnappr.common.zfs import Snapshot, ZfsCli, Peer, Dataset
from zfsnappr.common.command_utils import fetch_snaps, resolve_dataset_args, resolve_filter_args, get_peer, remove_peer
from zfsnappr.common.resolve_datasets import resolve_conn_datasets
from zfsnappr.common.parse_dataset_arg import parse_dataset_arg, ConnSpec
from zfsnappr.common.path import EMPTY_PATH
from zfsnappr.common.filter import SnapFilter
from zfsnappr.common.resolve_datasets import ResolvedDatasets
# from zfsnappr.common.replication.utils import 


log = logging.getLogger(__name__)

COLUMN_SEPARATOR = ' | '
HEADER_SEPARATOR = '-'

@dataclass
class Field:
    name: str
    get: Callable[[Snapshot], str]


def entrypoint(args: Args) -> None:
    resolved = resolve_dataset_args(args)
    resolved_dests = resolve_dataset_args(include_recurse=args.peer)

    # Dest must contain ALL datasets, otherwise we risk removing peers on source that actually exist
    assert all(dataset.p.is_all_matched for conn, (dataset, cli) in resolved_dests.items())

    peer_conn_guids = {conn: {p.guid for p in datasets.matched} for conn, (datasets, _) in resolved_dests.items()}

    # For each dataset, get all snapshots non-recursively
    _first = True
    for conn, (datasets, cli) in resolved.items():
        if not _first:
            log.info("")
        _first = False

        log.info(f"[{conn}] Syncing peers")
        sync_peer_conn(conn=conn, cli=cli, datasets=datasets, peer_conn_guids=peer_conn_guids)


def sync_peer_conn(conn: ConnSpec, cli: ZfsCli, datasets: ResolvedDatasets, peer_conn_guids: dict[ConnSpec, set[int]]):
    """
    - Check existing GUIDs on dest
    - Remove own peers
    - Remove holdtags on snapshots of those peers -> must get holdtags for all
    """
    for peer_conn, peer_guids in peer_conn_guids.items():
        # Check peer GUIDs and prune source
        expected_peers: dict[int, set[tuple[Dataset, Peer]]] = {}
        for ds in datasets.matched:
            for p in ds.peer_slots.values():
                if p and p.host == peer_conn:
                    expected_peers.setdefault(p.guid, set()).add((ds, p))

        obsolete_peers = {k: v for k, v in expected_peers.items() if k not in peer_guids}
        print(f"Found {len(obsolete_peers)} obsolete peers from {peer_conn}")
        for guid, _datasets in obsolete_peers.items():
            for ds, peer in _datasets:
                # remove_peer(cli=cli, dataset=d, peer_guid=guid)
                print(f"Removing peer {peer} on dataset {ds}")


def prune_unused_peers():
    """
    - Filter peers for age
    - Remove peers + holdtags
    """

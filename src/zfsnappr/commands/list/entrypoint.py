from __future__ import annotations
from typing import Optional, Callable, cast
from dataclasses import dataclass
from collections.abc import Collection, Mapping
import logging

from .args import Args
from zfsnappr.common.zfs import Snapshot, ZfsCli, PeerInfo, Dataset
from zfsnappr.common.command_utils import fetch_snaps, resolve_dataset_args, resolve_filter_args, get_peerinfo, get_holds
from zfsnappr.common.filter import SnapFilter
from zfsnappr.common.resolve_datasets import ResolvedDatasets
from zfsnappr.common.replication.utils import parse_holdtags, Direction
from zfsnappr.common.render_table import render_table, Field


log = logging.getLogger(__name__)


def entrypoint(args: Args) -> None:
    resolved = resolve_dataset_args(args)
    filter = resolve_filter_args(tag_groups=args.tag)

    # For each dataset, get all snapshots non-recursively
    _first = True
    for conn, (datasets, cli) in resolved.items():
        if not _first:
            log.info("")
        _first = False

        log.info(f"[{conn}] Scanning snapshots on {len(datasets.matched)} datasets")
        list_conn(cli=cli, datasets=datasets, filter=filter, extend_holds=args.show_holds, held_only=args.held_only)


def list_conn(cli: ZfsCli, datasets: ResolvedDatasets, filter: SnapFilter, extend_holds: bool, held_only: bool):
    snaps = fetch_snaps(cli, datasets, filter=filter)

    # get hold tags for all snapshots with holds
    holdtags = get_holds(cli, snaps)

    # Optionally filter snaps
    if held_only:
        snaps = [s for s in snaps if holdtags[s]]

    if not snaps:
        log.info(f"No matching snapshots")
        return

    fields: list[Field] = [
        Field('DATASET',    lambda s: str(s.dataset)),
        Field('SHORT NAME', lambda s: s.shortname),
        Field('TAGS',       lambda s: ','.join(sorted(s.tags)) if s.tags is not None else 'UNSET'),
        Field('TIMESTAMP',  lambda s: str(s.timestamp)),
    ]
    if extend_holds:
        fields += [Field('HOLDS', lambda s: "\n".join(sorted(holdtags[s])))]
    else:
        fields += [Field('HOLDS', lambda s: '+' if holdtags[s] else '')]
    fields += [Field('PEERS', lambda s: "\n".join(sorted(format_snap_peers(s, datasets, holdtags))))]

    render_table(fields, snaps)


def format_snap_peers(snapshot: Snapshot, datasets: ResolvedDatasets, holdtags: Mapping[Snapshot, Collection[str]]) -> list[str]:
    dataset = datasets.path_to_dataset[snapshot.dataset]
    tags = holdtags[snapshot]
    peers = {(hold.direction, get_peerinfo(dataset, hold.guid)) for hold in parse_holdtags(tags)}
    return [format_peerinfo(dir, p) for dir, p in peers]

def format_peerinfo(direction: Direction, peer: PeerInfo | None):
    match direction:
        case Direction.SEND:
            return f"Send to {peer.host}::{peer.path} ({peer.last_used})" if peer else "Send to unknown"
        case Direction.RECEIVE:
            return f"Receive from {peer.host}::{peer.path} ({peer.last_used})" if peer else "Receive from unknown"
        case _:
            assert False

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


log = logging.getLogger(__name__)

COLUMN_SEPARATOR = ' | '
HEADER_SEPARATOR = '-'

@dataclass
class Field:
    name: str
    get: Callable[[Snapshot], str]
    # whether to blank this column on wrapped lines
    blank_on_wrap: bool = False


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
        list_conn(cli=cli, datasets=datasets, filter=filter, extend_holds=args.holds)


def list_conn(cli: ZfsCli, datasets: ResolvedDatasets, filter: SnapFilter, extend_holds: bool):
    snaps = fetch_snaps(cli, datasets, filter=filter)
    if not snaps:
        log.info(f"No matching snapshots")
        return

    # get hold tags for all snapshots with holds
    holdtags = get_holds(cli, snaps)

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
    fields += [Field('PEERS', lambda s: "\n".join(sorted(get_snap_peers(s, datasets, holdtags))))]

    render_table(fields, snaps)


def get_snap_peers(snapshot: Snapshot, datasets: ResolvedDatasets, holdtags: Mapping[Snapshot, Collection[str]]) -> set[str]:
    dataset = datasets.path_to_dataset[snapshot.dataset]
    tags = holdtags[snapshot]
    peers = {(hold.direction, get_peerinfo(dataset, hold.guid)) for hold in parse_holdtags(tags)}
    return {format_peerinfo(dir, p) for dir, p in peers}

def format_peerinfo(direction: Direction, peer: PeerInfo | None):
    match direction:
        case Direction.SEND:
            return f"Send to {peer.host}::{peer.path} ({peer.last_used}) {peer.pool_guid}" if peer else "Send to unknown"
        case Direction.RECEIVE:
            return f"Receive from {peer.host}::{peer.path} ({peer.last_used}) {peer.pool_guid}" if peer else "Receive from unknown"
        case _:
            assert False


def render_table(fields: list[Field], snaps: list[Snapshot]) -> None:
    headers = [f.name for f in fields]

    # rows_blocks[row][col] = list of lines
    rows_blocks: list[list[list[str]]] = [
        [cell_lines(f.get(snap)) for f in fields]
        for snap in snaps
    ]

    # widths from the max visible line length in each column (including header)
    widths: list[int] = []
    for col, f in enumerate(fields):
        max_cell = 0
        for row in rows_blocks:
            max_cell = max(max_cell, max(len(line) for line in row[col]))
        widths.append(max(len(headers[col]), max_cell))

    total_width = (len(COLUMN_SEPARATOR) * (len(fields) - 1)) + sum(widths)

    # header
    log.info(COLUMN_SEPARATOR.join(h.ljust(w) for h, w in zip(headers, widths)))
    log.info((HEADER_SEPARATOR * (total_width // len(HEADER_SEPARATOR) + 1))[:total_width])

    # body
    for row in rows_blocks:
        row_height = max(len(cell) for cell in row)

        for i in range(row_height):
            parts: list[str] = []
            for col, f in enumerate(fields):
                cell = row[col]
                line = cell[i] if i < len(cell) else ""

                # blank columns on wrapped lines if requested
                if i > 0 and f.blank_on_wrap:
                    line = ""

                parts.append(line.ljust(widths[col]))

            log.info(COLUMN_SEPARATOR.join(parts))


def cell_lines(text: str) -> list[str]:
    # keep it simple; you can also handle \r\n etc if needed
    return text.splitlines() or [""]

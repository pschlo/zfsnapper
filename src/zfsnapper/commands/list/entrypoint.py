from __future__ import annotations
from typing import Optional, Callable, cast
from dataclasses import dataclass
from collections.abc import Collection, Mapping
import logging

from .args import Args
from zfsnapper.lib.zfs import Snapshot, ZfsCli, PeeringInfo, Dataset, Peering
from zfsnapper.lib.cli.command_utils import fetch_snaps, resolve_dataset_args, resolve_filter_args
from zfsnapper.lib.cli.snapfilter import SnapFilter
from zfsnapper.lib.cli.resolve_datasets import ResolvedDatasets
from zfsnapper.common.replication.utils import parse_holdtags, Direction
from zfsnapper.lib.cli.render_table import render_table, Field


log = logging.getLogger(__name__)

Field = Field[Snapshot]


def entrypoint(args: Args) -> None:
    resolved = resolve_dataset_args(args, default_all_local=True)
    filter = resolve_filter_args(match_tag_groups=args.tag)

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

    # Optionally filter snaps
    if held_only:
        snaps = [s for s in snaps if s.num_holds > 0]

    if not snaps:
        log.info(f"No matching snapshots")
        return

    fields = [
        Field('SHORTNAME', lambda s: s.shortname),
        Field('DATASET',    lambda s: str(s.dataset)),
        Field('TAGS',       lambda s: ','.join(sorted(s.tags)) if s.tags is not None else 'UNSET'),
        Field('TIMESTAMP',  lambda s: str(s.timestamp)),
    ]
    if extend_holds:
        fields += [Field('HOLDS', lambda s: "\n".join(sorted(s.holdtags)))]
    else:
        fields += [Field('HOLDS', lambda s: '+' if s.num_holds > 0 else '')]
    fields += [Field('PEERS', lambda s: "\n".join(sorted(format_peerholds(s, datasets))))]

    render_table(fields, [(s,) for s in snaps])


def format_peerholds(snapshot: Snapshot, datasets: ResolvedDatasets) -> list[str]:
    dataset = datasets.path_to_dataset[snapshot.dataset]
    return [format_peering(dataset, p) for p in snapshot.peerholds]

def format_peering(dataset: Dataset, peering: Peering):
    p = dataset.get_peerinfo(peering)
    return f"{peering.direction.icon}  {p.host if p else '?'}"

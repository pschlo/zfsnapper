from __future__ import annotations
from typing import Optional, Callable, cast
from dataclasses import dataclass
from collections.abc import Collection
import logging

from .args import Args
from zfsnappr.common.zfs import Snapshot, ZfsCli
from zfsnappr.common.command_utils import fetch_snaps, resolve_dataset_args, resolve_filter_args
from zfsnappr.common.filter import SnapFilter
from zfsnappr.common.resolve_datasets import ResolvedDatasets


log = logging.getLogger(__name__)

COLUMN_SEPARATOR = ' | '
HEADER_SEPARATOR = '-'

@dataclass
class Field:
    name: str
    get: Callable[[Snapshot], str]


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
    holdtags = cli.get_holdtags([s.longname for s in snaps], userrefs={s.longname: s.holds for s in snaps})

    fields: list[Field] = [
        Field('DATASET',    lambda s: str(s.dataset)),
        Field('SHORT NAME', lambda s: s.shortname),
        Field('TAGS',       lambda s: ','.join(sorted(s.tags)) if s.tags is not None else 'UNSET'),
        Field('TIMESTAMP',  lambda s: str(s.timestamp)),
    ]
    if extend_holds:
        fields += [Field('HOLDS', lambda s: ','.join(holdtags[s.longname]))]
    else:
        fields += [Field('HOLDS', lambda s: '+' if holdtags[s.longname] else '')]

    widths: list[int] = [max(len(f.name), *(len(f.get(s)) for s in snaps), 0) for f in fields]
    total_width = (len(COLUMN_SEPARATOR) * ((len(fields) or 1) - 1)) + sum(widths)

    log.info(COLUMN_SEPARATOR.join(f.name.ljust(w) for f, w in zip(fields, widths)))
    log.info((HEADER_SEPARATOR * (total_width//len(HEADER_SEPARATOR) + 1))[:total_width])
    for snap in snaps:
        log.info(COLUMN_SEPARATOR.join(f.get(snap).ljust(w) for f, w in zip(fields, widths)))

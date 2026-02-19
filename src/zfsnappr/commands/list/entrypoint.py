from __future__ import annotations
from typing import Optional, Callable, cast
from dataclasses import dataclass
from collections.abc import Collection
import logging

from .args import Args
from zfsnappr.common.zfs import Snapshot, ZfsCli
from zfsnappr.common.command_utils import fetch_snaps, resolve_dataset_args
from zfsnappr.common.resolve_datasets import ResolvedDatasets


log = logging.getLogger(__name__)

COLUMN_SEPARATOR = ' | '
HEADER_SEPARATOR = '-'

@dataclass
class Field:
  name: str
  get: Callable[[Snapshot], str]

# TODO: Use this list output for other subcommands as well

def entrypoint(args: Args) -> None:
  resolved = resolve_dataset_args(args)

  # For each dataset, get all snapshots non-recursively
  for i, (conn, (datasets, cli)) in enumerate(resolved.items()):
    log.info(f"Location: {conn}")
    list_conn(cli=cli, datasets=datasets, filter_tags=args.tag)
    if i < len(resolved)-1:
      log.info("")


def list_conn(cli: ZfsCli, datasets: ResolvedDatasets, filter_tags: Collection[str]):
    snaps = fetch_snaps(cli, datasets, filter_tags=filter_tags)
    if not snaps:
        log.info(f"No matching snapshots, nothing to do")
        return

    # get hold tags for all snapshots with holds
    holdtags = cli.get_holdtags([s.longname for s in snaps], userrefs={s.longname: s.holds for s in snaps})

    fields: list[Field] = [
      Field('DATASET',    lambda s: s.dataset),
      Field('SHORT NAME', lambda s: s.shortname),
      Field('TAGS',       lambda s: ','.join(s.tags) if s.tags is not None else 'UNSET'),
      Field('TIMESTAMP',  lambda s: str(s.timestamp)),
      Field('HOLDS',      lambda s: ','.join(holdtags[s.longname]))
    ]
    widths: list[int] = [max(len(f.name), *(len(f.get(s)) for s in snaps), 0) for f in fields]
    total_width = (len(COLUMN_SEPARATOR) * ((len(fields) or 1) - 1)) + sum(widths)

    log.info(COLUMN_SEPARATOR.join(f.name.ljust(w) for f, w in zip(fields, widths)))
    log.info((HEADER_SEPARATOR * (total_width//len(HEADER_SEPARATOR) + 1))[:total_width])
    for snap in snaps:
      log.info(COLUMN_SEPARATOR.join(f.get(snap).ljust(w) for f, w in zip(fields, widths)))

from __future__ import annotations
from typing import Optional, Callable, cast
from dataclasses import dataclass
import logging

from .args import Args
from zfsnappr.common.zfs import Snapshot, Hold, ZfsProperty, ZfsCli, Dataset
from zfsnappr.common.filter import filter_snaps, parse_tags
from zfsnappr.common.sort import sort_snaps_by_time
from zfsnappr.common.resolve_datasets import resolve_dataset_args, ResolvedDatasets


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
    print_list(cli=cli, datasets=datasets, args=args)
    if i < len(resolved)-1:
      log.info("")


def print_list(cli: ZfsCli, datasets: ResolvedDatasets, args: Args):
    snaps = [
      *cli.get_all_snapshots([g.name for g in datasets.recursive_groups], recursive=True),
      *cli.get_all_snapshots([d.name for d in datasets.single_datasets])
    ]
    snaps = filter_snaps(snaps, tag=parse_tags(args.tag))
    snaps = sort_snaps_by_time(snaps)


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

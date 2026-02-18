from __future__ import annotations
from typing import Optional, Callable, cast
from dataclasses import dataclass
import logging

from zfsnappr.common.zfs import Snapshot, Hold, ZfsProperty, ZfsCli, Dataset
from .args import Args
from collections import defaultdict
from zfsnappr.common.filter import filter_snaps, parse_tags
from zfsnappr.common.utils import parse_datasets, group_by, ConnectionSpec, create_zfs_clis, fullparse_datasets, fullparse_datasets_2
from zfsnappr.common.sort import sort_snaps_by_time


log = logging.getLogger(__name__)

COLUMN_SEPARATOR = ' | '
HEADER_SEPARATOR = '-'

@dataclass
class Field:
  name: str
  get: Callable[[Snapshot], str]

# TODO: Use this list output for other subcommands as well

def entrypoint(args: Args) -> None:
  fullparse_datasets_2(
      specs=args.dataset_spec,
      exclude_specs=args.exclude_dataset_spec,
      recursive=args.recursive
  )
  print("exiting")
  exit()

  datasets, clis = fullparse_datasets(
    specs=args.dataset_spec,
    exclude_specs=args.exclude_dataset_spec,
    recursive=args.recursive
  )
  if not datasets:
    log.info(f"No dataset locations specified, nothing to do")
    return

  # For each dataset, get all snapshots non-recursively
  for i, (conn, _datasets) in enumerate(datasets.items()):
    log.info(f"Location: {conn}")
    print_list(cli=clis[conn], datasets=[d.name for d in _datasets], args=args)
    if i < len(datasets)-1:
      log.info("")


def print_list(cli: ZfsCli, datasets: list[str], args: Args):
    snaps = cli.get_all_snapshots(datasets=datasets)
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

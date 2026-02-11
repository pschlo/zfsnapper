from __future__ import annotations
from typing import Optional, Callable, cast
from dataclasses import dataclass
import logging

from zfsnappr.common.zfs import Snapshot, Hold, ZfsProperty, ZfsCli, Dataset
from .args import Args
from collections import defaultdict
from zfsnappr.common.filter import filter_snaps, parse_tags
from zfsnappr.common.utils import parse_datasets, group_by, ConnectionSpec, create_zfs_clis
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
  raw_datasets = parse_datasets(args.dataset_spec)
  exclude_datasets = parse_datasets(args.exclude_dataset_spec)
  clis = create_zfs_clis(list(raw_datasets))

  datasets: dict[ConnectionSpec, list[Dataset]] = {}
  for conn, _datasets in raw_datasets.items():
    # Determine which datasets to fetch
    _fetch_datasets: list[str] | None
    if None in _datasets:
      if args.recursive:
        # Fetch everything
        _fetch_datasets = None
      else:
        raise ValueError(f"Cannot act on empty dataset path directly, must use recursion")
    else:
      _fetch_datasets = cast(list[str], _datasets)

    # Fetch datasets
    ds: list[Dataset] = clis[conn].get_all_datasets(_fetch_datasets, recursive=args.recursive)

    # Remove datasets that are excluded
    filtered_ds: list[Dataset] = []
    _exclude_ds = exclude_datasets.get(conn, [])
    if None in _exclude_ds and not args.recursive:
      raise ValueError(f"Cannot exclude empty dataset path directly, must use recursion")
    for d in ds:
      if args.recursive:
        # Recursive; check if prefix is excluded
        # The empty dataset (None) is a prefix of everything
        if any(x is None or d.name.startswith(x) for x in _exclude_ds):
          continue
      else:
        # Non-recursive; check if name is directly excluded
        if any(d.name == x for x in _exclude_ds):
          continue
      filtered_ds.append(d)

    datasets[conn] = filtered_ds

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

from __future__ import annotations
from typing import Optional, cast, Literal, Callable
from collections.abc import Collection
import logging

from zfsnappr.common.zfs import ZfsProperty, Snapshot, ZfsCli
from zfsnappr.common.resolve_datasets import ResolvedDatasets
from zfsnappr.common.filter import SnapFilter
from zfsnappr.common.command_utils import fetch_snaps, resolve_dataset_args, resolve_filter_args
from .args import Args


log = logging.getLogger(__name__)

TAG_SEPARATOR = "_"

type Operation = tuple[
    Callable[[Snapshot], Optional[set[str]]],
    Literal['ADD', 'SET', 'REMOVE']
]


def entrypoint(args: Args) -> None:
  resolved = resolve_dataset_args(args)
  filter = resolve_filter_args(tag_groups=args.tag, shortnames=args.snapshot)

  # --- determine operations ---
  operations: list[Operation] = []

  # TODO: remove
  ...

  # set
  if args.set_from_name:
    operations.append((get_from_name, 'SET'))
  if args.set_from_prop is not None:
    p = args.set_from_prop
    operations.append((lambda s: get_from_prop(s, p), 'SET'))

  # add
  if args.add_from_name:
    operations.append((get_from_name, 'ADD'))
  if args.add_from_prop is not None:
    p = args.add_from_prop
    operations.append((lambda s: get_from_prop(s, p), 'ADD'))

  if not operations:
    log.info(f"No tag operations specified, nothing to do")
    return

  # Apply tag command to each connection
  _first = True
  for conn, (datasets, cli) in resolved.items():
    if not _first:
      log.info("")
    _first = False

    log.info(f"Location: {conn}")
    tag_conn(
      cli=cli,
      datasets=datasets,
      operations=operations,
      fetch_props=[p for p in [args.add_from_prop, args.set_from_prop] if p is not None],
      filter=filter
    )


def tag_conn(
  cli: ZfsCli,
  datasets: ResolvedDatasets,
  operations: list[Operation],
  fetch_props: Collection[str],
  filter: SnapFilter
):
  # --- get snapshots ---
  snaps = fetch_snaps(cli, datasets, props=fetch_props, filter=filter)
  if not snaps:
      log.info(f"No matching snapshots, nothing to do")
      return

  # --- apply tag operations ---
  # SET sets the tags even if no new tags were found, while ADD and REMOVE leave the tags potentially unset, i.e. as None
  for snap in snaps:
    for get_tags, action in operations:
      tags = snap.tags
      new_tags = get_tags(snap)

      if action == 'SET':
        tags = new_tags or set()
      elif action == 'ADD' and new_tags is not None:
        tags = (tags or set()) | new_tags
      elif action == 'REMOVE' and new_tags is not None:
        tags = (tags or set()) - new_tags

      # apply tag changes
      if tags != snap.tags and tags is not None:
        cli.set_tags(snap.longname, tags)


def get_from_prop(snap: Snapshot, property: str) -> Optional[set[str]]:
  value = snap.properties[property]
  if value == '-':
    # property not set
    return None
  return set(t for t in value.split(',') if t)  # ignore empty tags

def get_from_name(snap: Snapshot) -> Optional[set[str]]:
  s = [a for a in snap.shortname.split(TAG_SEPARATOR) if a]  # ignore empty tags
  shortname_notags, tags = s[0], set(s[1:])
  if not tags:
    # no tags in name
    return None
  return tags

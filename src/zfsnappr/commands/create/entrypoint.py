from __future__ import annotations
from typing import Optional, cast
import random
import string
import logging

from zfsnappr.common.zfs import ZfsProperty, Dataset
from zfsnappr.common.resolve_datasets import resolve_datasets
from .args import Args


log = logging.getLogger(__name__)


def entrypoint(args: Args) -> None:
  plans, clis = resolve_datasets(
      include_exact=args.inc_dataset_exact,
      include_recurse=args.inc_dataset_recurse,
      exclude_exact=args.exc_dataset_exact,
      exclude_recurse=args.exc_dataset_recurse,
  )

  if not datasets:
    raise ValueError("No dataset locations specified, nothing to do")

  # Determine which subtrees can be snapshotted atomically
  # I.e. for each conn, group datasets such that:
  #   - datasets in a group share a prefix
  #   - no excluded dataset has this prefix
  #   - groups should be as large as possible, i.e. prefix as short as possible
  # Algorithm:
  #   - iterate over all prefixes, sorted by length in ascending order
  #   - for each prefix: check whether excluded. If so, discard prefix and continue
  # find all prefixes where there is nothing excluded
  groups: dict[str, set[Dataset]] = {}
  for conn, _datasets in datasets.items():
    _datasets_left = set(_datasets)
    all_prefixes: set[str] = set()
    for d in _datasets:
      parts = d.name.split('/')
      prefixes_parts = [parts[:i+1] for i in range(len(parts))]
      prefixes = {'/'.join(p) for p in prefixes_parts}
      all_prefixes |= prefixes

    for prefix in sorted(all_prefixes, key=lambda p: p.count('/')):
      matching_ds = {d for d in _datasets_left if d.name.startswith(prefix)}
      excluded_ds = ...
      if not excluded_ds:
        # Valid group
        groups[prefix] = matching_ds
        _datasets_left -= matching_ds
    assert not _datasets_left

  
  # generate random 10 digit alnum string
  #   10 digit alnum -> (26+26+10)^10 values = 839299365868340224 values = ca. 59.5 bit
  #   ZFS GUID (64 bits) -> 2^64 values = 18446744073709551616 values
  chars = string.ascii_lowercase + string.ascii_uppercase + string.digits
  shortname: str = ''.join(random.choices(chars, k=10))
  fullname = f'{dataset}@{shortname}'

  cli.create_snapshot(
    fullname=fullname,
    recursive=args.recursive,
    properties={
      ZfsProperty.CUSTOM_TAGS: ','.join(args.tag)
    }
  )

  log.info(f'Created snapshot {fullname}')

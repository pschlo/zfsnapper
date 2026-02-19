from __future__ import annotations
from typing import Optional, cast
import random
import string
import logging
from collections.abc import Collection

from zfsnappr.common.zfs import ZfsProperty, Dataset, ZfsCli
from zfsnappr.common.command_utils import resolve_dataset_args
from zfsnappr.common.resolve_datasets import ConnSpec
from .args import Args
from zfsnappr.common.sort import dataset_sortkey


log = logging.getLogger(__name__)


# generate random 10 digit alnum string
#   10 digit alnum -> (26+26+10)^10 values = 839299365868340224 values = ca. 59.5 bit
#   ZFS GUID (64 bits) -> 2^64 values = 18446744073709551616 values
CHARS = string.ascii_lowercase + string.ascii_uppercase + string.digits
def generate_random_name() -> str:
  return ''.join(random.choices(CHARS, k=10))


def entrypoint(args: Args) -> None:
  resolved = resolve_dataset_args(args)
  for conn, (datasets, cli) in resolved.items():
    atomic_creates = [
      *((d, False) for d in datasets.single_datasets),
      *((d, True) for d in datasets.recursive_groups)
    ]
    for dataset, recurse in sorted(atomic_creates, key=lambda t: dataset_sortkey(t[0])):
      create_snapshot(conn=conn, cli=cli, dataset=dataset, filter_tags=args.tag, recurse=recurse)


def create_snapshot(conn: ConnSpec, cli: ZfsCli, dataset: Dataset, filter_tags: Collection[str], recurse: bool):
  shortname = generate_random_name()
  fullname = f'{dataset.name}@{shortname}'

  cli.create_snapshot(
    fullname=fullname,
    recursive=recurse,
    properties={
      ZfsProperty.CUSTOM_TAGS: ','.join(filter_tags)
    }
  )

  log.info(f"Created{' recursive ' if recurse else ' '}snapshot of '{conn}/{dataset.name}': {shortname}")

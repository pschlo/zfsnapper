from __future__ import annotations
from typing import Optional, cast
import random
import string
import logging
from collections.abc import Collection

from zfsnappr.common.zfs import ZfsProperty, Dataset, ZfsCli
from zfsnappr.common.resolve_datasets import resolve_dataset_args, ResolvedDatasets, ConnSpec
from .args import Args


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
    for dataset in datasets.recursive_groups:
      create_snapshot(conn=conn, cli=cli, dataset=dataset, tags=args.tag, recurse=True)
    for dataset in datasets.single_datasets:
      create_snapshot(conn=conn, cli=cli, dataset=dataset, tags=args.tag, recurse=False)


def create_snapshot(conn: ConnSpec, cli: ZfsCli, dataset: Dataset, tags: Collection[str], recurse: bool):
  shortname = generate_random_name()
  fullname = f'{dataset.name}@{shortname}'

  cli.create_snapshot(
    fullname=fullname,
    recursive=recurse,
    properties={
      ZfsProperty.CUSTOM_TAGS: ','.join(tags)
    }
  )

  log.info(f"Created{' recursive ' if recurse else ' '}snapshot of '{conn}/{dataset.name}': {shortname}")

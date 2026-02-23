from __future__ import annotations
import random
import string
import logging

from zfsnappr.common.zfs import ZfsProperty
from zfsnappr.common.command_utils import resolve_dataset_args
from .args import Args
from zfsnappr.common.utils import space
from zfsnappr.common.sort import sortkey_dataset


log = logging.getLogger(__name__)


# generate random 10 digit alnum string
#   10 digit alnum -> (26+26+10)^10 values = 839299365868340224 values = ca. 59.5 bit
#   ZFS GUID (64 bits) -> 2^64 values = 18446744073709551616 values
CHARS = string.ascii_lowercase + string.ascii_uppercase + string.digits
def generate_random_name() -> str:
    return ''.join(random.choices(CHARS, k=10))


def entrypoint(args: Args) -> None:
    resolved = resolve_dataset_args(args)

    _first = True
    for conn, (datasets, cli) in resolved.items():
        if not _first:
            log.info("")
        _first = False

        shortname = generate_random_name()
        cli.create_snapshot(
            datasets=datasets.p.matched,
            shortname=shortname,
            properties={
                ZfsProperty.CUSTOM_TAGS: ','.join(args.tag)
            }
        )

        log.info(f"[{conn}] Created snapshot {shortname} of {len(datasets.matched)} datasets:")
        for dataset in sorted(datasets.matched, key=sortkey_dataset):
            log.info(space(1) + f"{dataset.path}")

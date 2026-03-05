from __future__ import annotations
from typing import Any, TypedDict, cast
import argparse

from .common.args import CommonArgs as CommonArgs
from .commands import (
  prune as _prune,
  create as _create,
  push as _push,
  list as _list,
  tag as _tag,
  unhold as _unhold,
  peer as _peer,
  version as _version
)


class Args(CommonArgs):
    subcommand: str


def get_args() -> Args:
    # Parent parser for global/common options
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument('-d', '--dataset', action='append', default=[], metavar="DATASET", dest="inc_dataset_exact")
    common.add_argument('-D', '--recurse-dataset', action='append', default=[], metavar="DATASET", dest="inc_dataset_recurse")
    common.add_argument('-x', '--exclude-dataset', action='append', default=[], metavar="DATASET", dest="exc_dataset_exact")
    common.add_argument('-X', '--recurse-exclude-dataset', action='append', default=[], metavar="DATASET", dest="exc_dataset_recurse")
    common.add_argument('-n', '--dry-run', action='store_true')
    common.add_argument('-s', '--strict', action='store_true')

    # create top-level parser
    parser = argparse.ArgumentParser(formatter_class=CompactHelpFormatter)
    subparsers = parser.add_subparsers(dest="subcommand", required=True)

    # create subcommand parsers
    _list.args.setup(
        subparsers.add_parser('list', parents=[common])
    )
    _create.args.setup(
        subparsers.add_parser('create', parents=[common])
    )
    _prune.args.setup(
        subparsers.add_parser('prune', parents=[common])
    )
    _push.args.setup(
        subparsers.add_parser('push', parents=[common])
    )
    _tag.args.setup(
        subparsers.add_parser('tag', parents=[common])
    )
    _unhold.args.setup(
        subparsers.add_parser('unhold', parents=[common])
    )
    _peer.args.setup(
        subparsers.add_parser('peer', parents=[common])
    )
    _version.args.setup(
        subparsers.add_parser('version')
    )

    # Optionally modify args
    args = dict(parser.parse_args()._get_kwargs())

    return cast(Args, argparse.Namespace(**args))


class CompactHelpFormatter(argparse.HelpFormatter):
    def __init__(self, prog):
        super().__init__(prog, max_help_position=40, width=120)

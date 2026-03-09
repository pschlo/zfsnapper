from __future__ import annotations
from dateutil.relativedelta import relativedelta
import re
from argparse import ArgumentParser

from zfsnapper.common.parse_duration import parse_duration
from zfsnapper.common.args import CommonArgs

from . import list as _list
from . import prune as _prune


class Args(CommonArgs):
    peer_command: str


def setup(parser: ArgumentParser, common: ArgumentParser) -> None:
    subparsers = parser.add_subparsers(dest="peer_command", required=True)

    _list.args.setup(
        subparsers.add_parser("list", parents=[common])
    )

    _prune.args.setup(
        subparsers.add_parser("prune", parents=[common])
    )

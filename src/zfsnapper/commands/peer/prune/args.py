from __future__ import annotations
from argparse import ArgumentParser

from zfsnapper.common.args import CommonArgs


class Args(CommonArgs):
    peer: list[str]
    from_: list[str]
    unused_for: str | None
    unheld: bool
    unknown: bool
    all: bool
    localhost: str | None


def setup(parser: ArgumentParser) -> None:
    parser.add_argument('peer', nargs='*', type=str)
    parser.add_argument('--from', action='append', default=[], metavar="HOSTNAME | HOSTNAME::POOL", dest="from_")
    parser.add_argument('--unused-for', metavar="DURATION", dest="unused_for")
    parser.add_argument('--unheld', action='store_true')
    parser.add_argument('--unknown', action='store_true')
    parser.add_argument('--all', action='store_true')
    parser.add_argument('--localhost', type=str)

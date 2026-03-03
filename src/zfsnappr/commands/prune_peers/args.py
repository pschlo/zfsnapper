from argparse import ArgumentParser

from zfsnappr.common.args import CommonArgs


class Args(CommonArgs):
    peer: list[str]
    from_: list[str]
    unused_for: str | None


def setup(parser: ArgumentParser) -> None:
    parser.add_argument('peer', nargs='*', type=str)
    parser.add_argument('--from', action='append', default=[], metavar="HOSTNAME | HOSTNAME::POOL", dest="from_")
    parser.add_argument('--unused-for', metavar="DURATION", dest="unused_for")

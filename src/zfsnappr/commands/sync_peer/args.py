from argparse import ArgumentParser

from zfsnappr.common.args import CommonArgs


class Args(CommonArgs):
    peer: list[str]


def setup(parser: ArgumentParser) -> None:
    parser.add_argument('peer', nargs='+', type=str)

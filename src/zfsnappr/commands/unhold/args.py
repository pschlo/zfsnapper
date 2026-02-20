from argparse import ArgumentParser

from zfsnappr.common.args import CommonArgs


class Args(CommonArgs):
    snapshot: list[str]


def setup(parser: ArgumentParser) -> None:
    parser.add_argument('snapshot', nargs='+', type=str)

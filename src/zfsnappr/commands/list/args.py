from argparse import ArgumentParser

from zfsnappr.common.args import CommonArgs


class Args(CommonArgs):
    tag: list[str]
    holds: bool


def setup(parser: ArgumentParser) -> None:
    parser.add_argument('-t', '--tag', type=str, action='append', default=[])
    parser.add_argument('--holds', action='store_true')

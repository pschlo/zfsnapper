from argparse import ArgumentParser

from zfsnappr.common.args import CommonArgs


class Args(CommonArgs):
    tag: list[str]
    show_holds: bool
    held_only: bool


def setup(parser: ArgumentParser) -> None:
    parser.add_argument('-t', '--tag', type=str, action='append', default=[])
    parser.add_argument('--show-holds', action='store_true')
    parser.add_argument('--held-only', action='store_true')

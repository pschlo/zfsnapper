from argparse import ArgumentParser

from zfsnappr.common.args import CommonArgs


class Args(CommonArgs):
  tag: list[str]


def setup(parser: ArgumentParser) -> None:
    parser.add_argument('-t', '--tag', type=str, action='append', default=[])

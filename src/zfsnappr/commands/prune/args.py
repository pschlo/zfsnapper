from __future__ import annotations
from dateutil.relativedelta import relativedelta
import re
from argparse import ArgumentParser

from zfsnappr.common.parse_duration import parse_duration
from zfsnappr.common.args import CommonArgs


class Args(CommonArgs):
    # Filter options
    tag: list[str]
    snapshot: list[str]

    keep_last: int
    keep_hourly: int
    keep_daily: int
    keep_weekly: int
    keep_monthly: int
    keep_yearly: int

    keep_within: relativedelta
    keep_within_hourly: relativedelta
    keep_within_daily: relativedelta
    keep_within_weekly: relativedelta
    keep_within_monthly: relativedelta
    keep_within_yearly: relativedelta

    keep_name: re.Pattern
    group_by: str
    keep_tag: list[str]


COUNT_OPTS = [
    "--keep-last",
    "--keep-hourly",
    "--keep-daily",
    "--keep-weekly",
    "--keep-monthly",
    "--keep-yearly"
]

WITHIN_OPTS = [
    "--keep-within",
    "--keep-within-hourly",
    "--keep-within-daily",
    "--keep-within-weekly",
    "--keep-within-monthly",
    "--keep-within-yearly"
]


def setup(parser: ArgumentParser) -> None:
    # filter snapshots by tag
    parser.add_argument('-t', '--tag', type=str, action='append', default=[])

    # keep policy arguments
    for opt in COUNT_OPTS:
        parser.add_argument(opt, type=int, metavar="N", default=0)
    for opt in WITHIN_OPTS:
        parser.add_argument(opt, type=parse_duration, metavar="DURATION", default=relativedelta())
    parser.add_argument('--keep-name', type=re.compile, metavar="REGEX")
    parser.add_argument('--group-by', type=str, metavar='GROUP', choices={'', 'dataset'}, default='dataset')
    parser.add_argument('--keep-tag', type=str, action='append', default=[])

    # filter snapshots by name
    parser.add_argument('snapshot', nargs='*', type=str)

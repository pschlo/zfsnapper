from __future__ import annotations
from dateutil.relativedelta import relativedelta
import re
from argparse import ArgumentParser

from zfsnappr.common.parse_duration import parse_duration
from zfsnappr.common.args import CommonArgs


class Args(CommonArgs):
    pass


def setup(parser: ArgumentParser) -> None:
    pass

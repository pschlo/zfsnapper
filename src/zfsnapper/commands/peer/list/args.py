from __future__ import annotations
from dateutil.relativedelta import relativedelta
import re
from argparse import ArgumentParser

from zfsnapper.common.parse_duration import parse_duration
from zfsnapper.common.args import CommonArgs


class Args(CommonArgs):
    pass


def setup(parser: ArgumentParser) -> None:
    pass

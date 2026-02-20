from __future__ import annotations
from dataclasses import dataclass
from typing import Optional
from argparse import ArgumentParser

from zfsnappr.common.args import CommonArgs


@dataclass
class Args(CommonArgs):
    pass


def setup(parser: ArgumentParser) -> None:
    pass

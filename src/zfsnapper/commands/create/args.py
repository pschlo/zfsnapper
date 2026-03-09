from __future__ import annotations
from dataclasses import dataclass
from typing import Optional
from argparse import ArgumentParser

from zfsnapper.common.args import CommonArgs


class Args(CommonArgs):
    tag: list[str]


def setup(parser: ArgumentParser) -> None:
    parser.add_argument('-t', '--tag', action='append', default=[])

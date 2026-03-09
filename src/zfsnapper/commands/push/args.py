from __future__ import annotations
from dataclasses import dataclass
from typing import Optional
from argparse import ArgumentParser

from zfsnapper.common.args import CommonArgs


class Args(CommonArgs):
    dest: str
    init: bool
    rollback: bool
    localhost: str | None


def setup(parser: ArgumentParser) -> None:
    parser.add_argument('dest', metavar='USER@HOST:PORT::DATASET')
    parser.add_argument('--init', action='store_true')
    parser.add_argument('--rollback', action='store_true')
    parser.add_argument('--localhost', type=str)

from __future__ import annotations
from dataclasses import dataclass
from typing import Optional
from argparse import ArgumentParser

from zfsnappr.common.args import CommonArgs


class Args(CommonArgs):
  # filter options
  tag: list[str]

  # extraction options
  set_from_prop: Optional[str]
  add_from_prop: Optional[str]
  set_from_name: bool
  add_from_name: bool

  snapshot: list[str]


def setup(parser: ArgumentParser) -> None:
  parser.add_argument('-t', '--tag', type=str, action='append', default=[])

  group = parser.add_mutually_exclusive_group()
  group.add_argument('--set-from-prop', metavar='PROP')
  group.add_argument('--set-from-name', action='store_true')

  parser.add_argument('--add-from-prop', metavar='PROP')
  parser.add_argument('--add-from-name', action='store_true')

  parser.add_argument('snapshot', nargs='*', type=str)

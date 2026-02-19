from __future__ import annotations
from typing import Protocol


class CommonArgs(Protocol):
  inc_dataset_exact: list[str]
  inc_dataset_recurse: list[str]
  exc_dataset_exact: list[str]
  exc_dataset_recurse: list[str]
  dry_run: bool
  strict: bool

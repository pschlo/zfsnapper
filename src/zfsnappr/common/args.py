from __future__ import annotations
from typing import Protocol


class CommonArgs(Protocol):
  dataset_spec: list[str]
  exclude_dataset_spec: list[str]
  recursive: bool
  dry_run: bool

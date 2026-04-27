from __future__ import annotations
from dataclasses import dataclass, field, replace
from collections.abc import Collection
from subprocess import Popen
from typing import Any, Literal

from .model import ZfsModel
from .runner import CommandRunner
from .base import BackendCommand, LimitedSnapInfo


@dataclass
class ZfsBackend:
    _model: ZfsModel
    _runner: CommandRunner
    _history: list[BackendCommand]

    def execute(self, cmd: BackendCommand):
        result = cmd.execute(self._runner)
        self._history.append(cmd)
        cmd.project(result, self._model)
        return result
    
    def find_snap(self, dataset: int | str | Path, snap: int | str) -> LimitedSnapInfo:
        return self._model.find_snapshot(dataset=dataset, snap=snap)

    def find_snaps(self, datasets: int | str | Path | Collection[int | str | Path]) -> list[LimitedSnapInfo]:
        return self._model.find_snapshots(datasets=datasets)
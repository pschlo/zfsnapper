from __future__ import annotations

from dataclasses import dataclass, field, replace
from subprocess import Popen
from typing import Any, Literal

from .model import ZfsModel
from .runner import CommandRunner
from .base import BackendCommand


@dataclass
class ZfsBackend:
    _model: ZfsModel
    _runner: CommandRunner
    _history: list[BackendCommand]

    def execute(self, cmd: BackendCommand):
        result = cmd.execute(self._runner)
        self._history.append(cmd)
        self._model = cmd.project(result, self._model)
        return result

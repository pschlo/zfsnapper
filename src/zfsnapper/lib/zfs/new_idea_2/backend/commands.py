from __future__ import annotations

from dataclasses import dataclass, field, replace
from subprocess import Popen
from typing import Any, Literal

from .runner import CommandRunner
from .model import ZfsModel
from .base import BackendCommand, LimitedSnapInfo


@dataclass
class CommandA(BackendCommand):
    foo1: str = "..."
    foo2: str = "..."

    def execute(self, runner: CommandRunner) -> str:
        return runner.send_command(...)
    
    def project(self, result, model: ZfsModel) -> ZfsModel:
        return model


@dataclass
class CommandB(BackendCommand):
    def execute(self, runner: CommandRunner):
        return runner.send_command(...)


@dataclass
class FetchSnapshots(BackendCommand[list[LimitedSnapInfo]]):
    def execute(self, runner: CommandRunner):
        res = runner.send_command(...)
        # Convert lines into row objects
        return [LimitedSnapInfo()]

    def project(self, result, model) -> ZfsModel:
        return replace(model, snapshots=model.snapshots + result)

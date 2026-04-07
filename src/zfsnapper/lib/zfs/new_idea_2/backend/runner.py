from __future__ import annotations

from dataclasses import dataclass, field, replace
from subprocess import Popen
from typing import Any, Literal


@dataclass
class CommandRunner:
    _history: list[tuple[str, ...]]

    def send_command(self, cmd: list[str]) -> str:
        self._history.append(
            tuple(cmd)
        )
        return "foobar"

from __future__ import annotations
import logging
from dataclasses import dataclass, field, replace
from subprocess import Popen, CalledProcessError, PIPE
from abc import ABC, abstractmethod
import shlex
from typing import Any, Literal


log = logging.getLogger(__name__)


class CommandRunner(ABC):
    @abstractmethod
    def start_command(self, cmd: list[str], stdin=None, stdout=None, stderr=None, text=False) -> Popen: ...

    def run_text_command(self, cmd: list[str]) -> str:
        p: Popen[str] = self.start_command(cmd, stdout=PIPE, text=True)
        stdout, _ = p.communicate()
        if p.returncode != 0:
            raise CalledProcessError(p.returncode, cmd=p.args, output=stdout)
        return stdout


class LocalCommandRunner(CommandRunner):
    def start_command(self, cmd: list[str], stdin=None, stdout=None, stderr=None, text=False) -> Popen:
        log.debug(f"Running local command: {' '.join(cmd)}")
        return Popen(cmd, stdin=stdin, stdout=stdout, stderr=stderr, text=text)


class SshCommandRunner(CommandRunner):
    ssh_command: list[str]

    def __init__(self, host: str, user: str | None, port: int | None) -> None:
        super().__init__()

        cmd = [
            "ssh",
            "-o", "ControlMaster=auto",
            "-o", "ControlPersist=5m",
            "-o", "ControlPath=~/.ssh/cm-%C",
        ]
        if user is not None:
            cmd += ['-l', user]
        if port is not None:
            cmd += ['-p', str(port)]
        cmd += [host]
        self.ssh_command = cmd

    def start_command(self, cmd: list[str], stdin=None, stdout=None, stderr=None, text=False) -> Popen:
        log.debug(f"Running ssh command: {' '.join(cmd)}")
        remote_cmd = ' '.join(shlex.quote(arg) for arg in cmd)
        return Popen(
            self.ssh_command + [remote_cmd],
            stdin=stdin,
            stdout=stdout,
            stderr=stderr,
            text=text
        )

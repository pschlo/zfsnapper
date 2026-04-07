from dataclasses import dataclass

from .runner import CommandRunner
from .model import ZfsModel


class BackendCommand[T]:
    """
    one concrete ZFS call + model projection

    - Results in exactly one ZFS command execution
    - Updates the model accordingly
    """
    def execute(self, runner: CommandRunner) -> T: ...
    def project(self, result: T, model: ZfsModel) -> ZfsModel: ...


class LimitedSnapInfo: ...

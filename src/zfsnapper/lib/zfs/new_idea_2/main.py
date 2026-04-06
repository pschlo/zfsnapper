from __future__ import annotations
from dataclasses import dataclass, field, replace
from subprocess import Popen
from typing import Any, Literal



@dataclass
class ZfsModel:
    pass

@dataclass
class CommandRunner:
    _history: list[tuple[str, ...]]

    def send_command(self, cmd: list[str]) -> str:
        self._history.append(
            tuple(cmd)
        )
        return "foobar"



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



###########################

class BackendCommand[T]:
    """
    - Results in exactly one ZFS command execution
    - Updates the model accordingly
    """
    def execute(self, runner: CommandRunner) -> T: ...
    def project(self, result: T, model: ZfsModel) -> ZfsModel: ...


@dataclass
class BackendCommandA(BackendCommand):
    foo1: str = "..."
    foo2: str = "..."

    def execute(self, runner: CommandRunner) -> str:
        return runner.send_command(...)
    
    def project(self, result, model: ZfsModel) -> ZfsModel:
        return model


@dataclass
class BackendCommandB(BackendCommand):
    def execute(self, runner: CommandRunner):
        return runner.send_command(...)


class LimitedSnapInfo: ...

@dataclass
class BackendFetchSnapshots(BackendCommand[list[LimitedSnapInfo]]):
    def execute(self, runner: CommandRunner):
        res = runner.send_command(...)
        # Convert lines into row objects
        return [LimitedSnapInfo()]

    def project(self, result, model) -> ZfsModel:
        return replace(model, snapshots=model.snapshots + result)


###########################


class DomainCommand[T]:
    """
    - May build response from backend model
    - May execute any number of backend commands
    """
    def execute(self, backend: ZfsBackend, refresh: bool | None = None) -> T:
        """
        - `refresh`: If True, force backend command execution. If False, never execute backend command (may raise exception). If None, refresh if needed.
        """
        ...

@dataclass
class DomainCommand1(DomainCommand):
    def execute(self, backend: ZfsBackend, refresh: bool | None = None):
        backend.execute(BackendCommandA())
        backend.execute(BackendCommandB())

@dataclass
class DomainCommand2(DomainCommand):
    def execute(self, backend: ZfsBackend, refresh: bool | None = None):
        backend.execute(BackendCommandB())
        backend.execute(BackendCommandB())
        backend.execute(BackendCommandB())

@dataclass
class FullSnapInfo: ...

@dataclass
class DomainFetchSnapshot(DomainCommand[FullSnapInfo]):
    """Fetch a single snapshot."""
    name: str

    def execute(self, backend: ZfsBackend, refresh: bool | None = None) -> FullSnapInfo:
        # Do lookups in backend's model to gather information about the snapshot
        # Construct FullSnapInfo
        return FullSnapInfo()

@dataclass
class DomainFetchSnapshots(DomainCommand[list[FullSnapInfo]]):
    """Fetch all snapshots."""
    def execute(self, backend: ZfsBackend, refresh: bool | None = None) -> list[FullSnapInfo]:
        # Either we construct response from backend's model,
        # or we execute backend commands and construct response from either backend command response or the new model
        limited_snap_infos: list[LimitedSnapInfo] = backend.execute(BackendFetchSnapshots())  # or lookup model
        holds: dict[int, set[str]] = {}  # execute backend command or lookup model
        parent_guids: Any  # execute backend command or lookup model

        # For each snapshot, construct a FullSnapInfo.
        # For this we can utilize ComplexFetchSnapshot command and disable refresh
        # (we have just updated the model for all snapshots in a single command, which is more efficient; the model should be up to date)
        snaps: list[str] = [...]
        return [
            DomainFetchSnapshot(snap).execute(backend, refresh=False)
            for snap in snaps
        ]


###########################


@dataclass
class ZfsManager:
    _backend: ZfsBackend
    _history: list[DomainCommand]

    def execute[T](self, cmd: DomainCommand[T]) -> T:
        result = cmd.execute(self._backend)
        self._history.append(cmd)
        return result


    ## Convenience methods

    def complex_command_1(self):
        return self.execute(
            DomainCommand2()
        )

    def complex_command_2(self):
        return self.execute(
            DomainCommand2()
        )

    def snapshot(self, name: str):
        return self.execute(
            DomainFetchSnapshot(name)
        )

    def snapshots(self):
        return self.execute(
            DomainFetchSnapshots()
        )

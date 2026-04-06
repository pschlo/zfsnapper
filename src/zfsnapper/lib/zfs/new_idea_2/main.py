from __future__ import annotations

from dataclasses import dataclass, field, replace
from subprocess import Popen
from typing import Any, Literal



@dataclass
class SnapshotRef:
    """Essentially a `DomainFetchSnapshot` command wrapper. Extension/Handler of a ZfsManager."""
    guid: int
    manager: ZfsManager
    refresh_on_resolve: bool | None = None
    """Whether the snapshot will be fetched if it does not exist in the model cache."""

    def resolve(self) -> FullSnapInfo:
        # Fetch the snapshot via public manager API
        return self.manager.snapshot(name="...", refresh=self.refresh_on_resolve)


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
    def execute(self, backend: ZfsBackend, *, refresh: bool | None = None) -> T:
        """
        - `refresh`: If True, force backend command execution. If False, never execute backend command (may raise exception). If None, refresh if needed.
        """
        ...

@dataclass
class DomainCommand1(DomainCommand):
    def execute(self, backend: ZfsBackend, *, refresh: bool | None = None):
        backend.execute(BackendCommandA())
        backend.execute(BackendCommandB())

@dataclass
class DomainCommand2(DomainCommand):
    def execute(self, backend: ZfsBackend, *, refresh: bool | None = None):
        backend.execute(BackendCommandB())
        backend.execute(BackendCommandB())
        backend.execute(BackendCommandB())

@dataclass
class FullSnapInfo:
    name: str = "dummy"
    guid: int = 0


@dataclass
class DomainFetchSnapshot(DomainCommand[FullSnapInfo]):
    """Fetch a single snapshot."""
    name: str

    def execute(self, backend: ZfsBackend, *, refresh: bool | None = None) -> FullSnapInfo:
        # Do lookups in backend's model to gather information about the snapshot
        # Construct FullSnapInfo
        return FullSnapInfo()


@dataclass
class DomainFetchSnapshotRef(DomainCommand[SnapshotRef]):
    manager: ZfsManager
    name: str
    refresh_on_resolve: bool | None
    """Passed to `SnapshotRef`."""

    def execute(self, backend: ZfsBackend, *, refresh: bool | None = None) -> Any:
        # Must get GUID for stable identification
        snap_info = DomainFetchSnapshot(self.name).execute(backend, refresh=refresh)
        return SnapshotRef(guid=snap_info.guid, manager=self.manager, refresh_on_resolve=self.refresh_on_resolve)


@dataclass
class DomainFetchSnapshots(DomainCommand[list[FullSnapInfo]]):
    """Fetch all snapshots."""
    def execute(self, backend: ZfsBackend, *, refresh: bool | None = None) -> list[FullSnapInfo]:
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


@dataclass
class DomainFetchSnapshotsRefs(DomainCommand[list[SnapshotRef]]):
    manager: ZfsManager
    refresh_on_resolve: bool | None
    """Passed to `SnapshotRef`."""

    def execute(self, backend: ZfsBackend, *, refresh: bool | None = None) -> Any:
        # Must get GUID for stable identification
        snap_infos = DomainFetchSnapshots().execute(backend, refresh=refresh)
        return [
            SnapshotRef(guid=snap_info.guid, manager=self.manager, refresh_on_resolve=self.refresh_on_resolve)
            for snap_info in snap_infos
        ]



###########################


@dataclass
class ZfsManager:
    _backend: ZfsBackend
    _history: list[DomainCommand]

    def execute[T](self, cmd: DomainCommand[T], *, refresh: bool | None = None) -> T:
        result = cmd.execute(self._backend, refresh=refresh)
        self._history.append(cmd)
        return result


    ## Convenience methods

    def complex_command_1(self, *, refresh: bool | None = None):
        return self.execute(
            DomainCommand2(),
            refresh=refresh
        )

    def complex_command_2(self, *, refresh: bool | None = None):
        return self.execute(
            DomainCommand2(),
            refresh=refresh
        )

    def snapshot(self, name: str, *, refresh: bool | None = None):
        return self.execute(
            DomainFetchSnapshot(name),
            refresh=refresh
        )

    def snapshot_ref(self, name: str, refresh_on_resolve: bool | None = None, *, refresh: bool | None = None):
        return self.execute(
            DomainFetchSnapshotRef(
                manager=self,
                name=name,
                refresh_on_resolve=refresh_on_resolve
            ),
            refresh=refresh
        )

    def snapshots(self, *, refresh: bool | None = None):
        return self.execute(
            DomainFetchSnapshots(),
            refresh=refresh
        )

    def snapshots_refs(self, refresh_on_resolve: bool | None = None, *, refresh: bool | None = None):
        return self.execute(
            DomainFetchSnapshotsRefs(
                manager=self,
                refresh_on_resolve=refresh_on_resolve
            ),
            refresh=refresh
        )

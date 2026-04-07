from __future__ import annotations

from dataclasses import dataclass, field, replace
from subprocess import Popen
from typing import Any, Literal


from .base import DomainCommand, FullSnapInfo
from ..backend import ZfsBackend, commands as backend_commands, LimitedSnapInfo


@dataclass
class Command1(DomainCommand):
    def execute(self, backend: ZfsBackend, *, refresh: bool | None = None):
        backend.execute(backend_commands.CommandA())
        backend.execute(backend_commands.CommandB())

@dataclass
class Command2(DomainCommand):
    def execute(self, backend: ZfsBackend, *, refresh: bool | None = None):
        backend.execute(backend_commands.CommandB())
        backend.execute(backend_commands.CommandB())
        backend.execute(backend_commands.CommandB())


@dataclass
class FetchSnapshot(DomainCommand[FullSnapInfo]):
    """Fetch a single snapshot."""
    name: str

    def execute(self, backend: ZfsBackend, *, refresh: bool | None = None) -> FullSnapInfo:
        # Do lookups in backend's model to gather information about the snapshot
        # Construct FullSnapInfo
        return FullSnapInfo()


@dataclass
class FetchSnapshots(DomainCommand[list[FullSnapInfo]]):
    """Fetch all snapshots."""
    def execute(self, backend: ZfsBackend, *, refresh: bool | None = None) -> list[FullSnapInfo]:
        # Either we construct response from backend's model,
        # or we execute backend commands and construct response from either backend command response or the new model
        limited_snap_infos: list[LimitedSnapInfo] = backend.execute(backend_commands.FetchSnapshots())  # or lookup model
        holds: dict[int, set[str]] = {}  # execute backend command or lookup model
        parent_guids: Any  # execute backend command or lookup model

        # For each snapshot, construct a FullSnapInfo.
        # For this we can utilize ComplexFetchSnapshot command and disable refresh
        # (we have just updated the model for all snapshots in a single command, which is more efficient; the model should be up to date)
        snaps: list[str] = [...]
        return [
            FetchSnapshot(snap).execute(backend, refresh=False)
            for snap in snaps
        ]

from __future__ import annotations

from dataclasses import dataclass, field, replace
from subprocess import Popen
from typing import Any, Literal, TYPE_CHECKING

from ..backend import ZfsBackend
from . import commands as domain_commands
from .base import DomainCommand, FullSnapInfo


@dataclass
class ZfsManager:
    """ergonomic façade for domain commands."""
    _backend: ZfsBackend
    _history: list[DomainCommand]

    def _execute[T](self, cmd: DomainCommand[T], *, refresh: bool | None = None) -> T:
        result = cmd.execute(self._backend, refresh=refresh)
        self._history.append(cmd)
        return result


    ####### Convenience methods

    def complex_command_1(self, *, refresh: bool | None = None):
        return self._execute(
            domain_commands.Command2(),
            refresh=refresh
        )

    def complex_command_2(self, *, refresh: bool | None = None):
        return self._execute(
            domain_commands.Command2(),
            refresh=refresh
        )

    def snapshot(self, name: str, *, refresh: bool | None = None):
        return self._execute(
            domain_commands.FetchSnapshot(name),
            refresh=refresh
        )

    def snapshots(self, *, refresh: bool | None = None):
        return self._execute(
            domain_commands.FetchSnapshots(),
            refresh=refresh
        )


    ####### Ref/Handler commands

    def snapshot_ref(self, name: str, refresh_on_resolve: bool | None = None, *, refresh: bool | None = None):
        # Must get GUID for stable identification
        snap_info = self._execute(
            domain_commands.FetchSnapshot(name),
            refresh=refresh
        )
        return SnapshotRef(_guid=snap_info.guid, _manager=self, _refresh_on_resolve=refresh_on_resolve)

    def snapshots_refs(self, refresh_on_resolve: bool | None = None, *, refresh: bool | None = None):
        # Must get GUID for stable identification
        snap_infos = self._execute(
            domain_commands.FetchSnapshots(),
            refresh=refresh
        )
        return [
            SnapshotRef(_guid=snap_info.guid, _manager=self, _refresh_on_resolve=refresh_on_resolve)
            for snap_info in snap_infos
        ]


@dataclass
class SnapshotRef:
    """lazy handles to snapshots; belong to a ZfsManager."""
    _guid: int
    _manager: ZfsManager
    _refresh_on_resolve: bool | None = None
    """Whether the snapshot will be fetched if it does not exist in the model cache."""

    def resolve(self) -> FullSnapInfo:
        # Fetch the snapshot via public manager API
        return self._manager.snapshot(name="...", refresh=self._refresh_on_resolve)

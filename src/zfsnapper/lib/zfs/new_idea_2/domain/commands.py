from __future__ import annotations

from dataclasses import dataclass, field, replace
from subprocess import Popen
from typing import Any, Literal


from .base import DomainCommand, FullSnapInfo
from ..backend import ZfsBackend, commands as backend_commands, LimitedSnapInfo, UniqueSnapKey


@dataclass
class FetchSnapshot(DomainCommand[FullSnapInfo]):
    """Fetch a single snapshot."""
    longname: str | None
    shortname: int | str | None
    dataset: int | str | None
    key: UniqueSnapKey | None

    def __post_init__(self):
        # Validation
        if sum([
            self.key is not None,
            self.longname is not None,
            self.dataset is not None or self.shortname is not None
        ]) > 1:
            raise ValueError(f"Key, longname and (dataset, shortname) are mutually exclusive")

        if sum([
            self.key is not None,
            self.longname is not None,
            self.dataset is not None and self.shortname is not None
        ]) == 0:
            raise ValueError(f"Insufficient data")

    def execute(self, backend: ZfsBackend, *, refresh: bool | None = None) -> FullSnapInfo:
        # Do lookups in backend's model to gather information about the snapshot
        # Construct FullSnapInfo
        
        # Check if we can construct from model

        # First, try to find LimitedSnapInfo. If not found, fetch snap.
        info: LimitedSnapInfo
        if self.key is not None:
            info = backend.find_snap(dataset=self.key.dataset_guid, snap=self.key.snapshot_guid)
        elif self.longname is not None:
            dataset, shortname = self.longname.split('@')
            info = backend.find_snap(dataset=dataset, snap=shortname)
        elif self.dataset is not None and self.shortname is not None:
            info = backend.find_snap(dataset=self.dataset, snap=self.shortname)
        else:
            assert False

        # Then, try to find hold information. If not found, fetch holds.
        ...

        # Then, try to find parent dataset GUID. If not found, fetch.
        ...

        # Finally, construct FullSnapInfo.

        return FullSnapInfo()


@dataclass
class FetchSnapshots(DomainCommand[list[FullSnapInfo]]):
    """Fetch all snapshots for the given datasets."""
    dataset: int | str | Path

    def execute(self, backend: ZfsBackend, *, refresh: bool | None = None) -> list[FullSnapInfo]:
        # Either we construct response from backend's model,
        # or we execute backend commands and construct response from either backend command response or the new model
        limited_snap_infos: list[LimitedSnapInfo] = backend.execute(
            backend_commands.GetSnapshots(
                datasets=self.dataset
            )
        )  # or lookup model
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

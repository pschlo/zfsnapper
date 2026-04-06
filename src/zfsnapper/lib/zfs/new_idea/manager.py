from __future__ import annotations
from dataclasses import dataclass, field
from subprocess import Popen

from .model import ZfsModel, SnapshotInfo, DatasetInfo
from .backend import ZfsBackend
from . import commands as commands
from .commands import ModelCommand


class ZfsManager:
    _model: ZfsModel
    _backend: ZfsBackend

    def __init__(self) -> None:
        self._backend = ZfsBackend()
        self._model = ZfsModel()

    def _resolve_snap_guid(self, snapshot: str | SnapshotRef) -> int:
        if isinstance(snapshot, SnapshotRef):
            return snapshot.guid
        try:
            return self._model.snap_name_to_guid[snapshot]
        except KeyError:
            raise ValueError(f"Unknown snapshot: {snapshot!r}") from None


    ### Method type 1: Query backend and merge into model

    def load_dataset(self, dataset: str | DatasetRef) -> list[DatasetRef]: ...
    def load_snapshots(self, snapshots: str | SnapshotRef) -> list[SnapshotRef]: ...

    ### Method type 2: Read from current model only

    def known_datasets(self, dataset: str | DatasetRef) -> list[DatasetRef]: ...
    def known_snapshots(self, dataset: str | SnapshotRef) -> list[SnapshotRef]: ...

    ### Method type 3: Execute mutations

    def hold_snapshot(self, snapshot: str | SnapshotRef, tag: str) -> None:
        cmd = commands.HoldSnapshot(
            snapshot_guid=self._resolve_snap_guid(snapshot),
            tag=tag
        )
        self._execute(cmd)

    def _execute[T](self, cmd: ModelCommand[T]) -> T:
        backend_cmd = cmd.compile(self._model)
        result = backend_cmd.execute(self._backend)
        self._model = cmd.project(self._model)
        return result


    def send_snapshot_async(
        self,
        snap: T_Snap,
        raw: bool,
        base: T_Snap | None = None,
        include_intermediates: bool = False,
        props: bool = False,
        no_preserve_encryption: bool = False
    ) -> Popen[bytes]:
        cmd = commands.SendSnapshot(
            snap_guid=self._resolve_snap_guid(snap),
            raw=raw,
            base_guid=self._resolve_snap_guid(base) if base is not None else None,
            include_intermediates=include_intermediates,
            props=props,
            no_preserve_encryption=no_preserve_encryption
        )
        return self._execute(cmd)


@dataclass(frozen=True, slots=True)
class SnapshotRef:
    guid: int
    manager: ZfsManager = field(repr=False, compare=False)

    def info(self) -> SnapshotInfo:
        ...


@dataclass(frozen=True, slots=True)
class DatasetRef:
    guid: int
    manager: ZfsManager = field(repr=False, compare=False)

    def info(self) -> DatasetInfo:
        ...


T_Snap = str | SnapshotRef

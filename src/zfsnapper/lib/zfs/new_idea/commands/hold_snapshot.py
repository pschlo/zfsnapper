from dataclasses import dataclass

from ._base import ModelCommand, BackendCommand
from ..model import ZfsModel
from ..backend import ZfsBackend


@dataclass(frozen=True, slots=True)
class ModelHoldSnapshot(ModelCommand):
    snapshot_guid: int
    tag: str

    def compile(self, model: ZfsModel) -> BackendCommand:
        snap = model.snapshots[self.snapshot_guid]
        return BackendHoldSnapshot(snapshot_name=snap.name, tag=self.tag)

    def project(self, model: ZfsModel) -> ZfsModel:
        snap = model.snapshots.get(self.snapshot_guid)
        if snap is None:
            return model

        new_snapshots = dict(model.snapshots)
        new_snapshots[snap.guid] = snap.with_(
            holdtags=snap.holdtags | {self.tag}
        )
        return model.with_(
            snapshots=new_snapshots
        )


@dataclass(frozen=True, slots=True)
class BackendHoldSnapshot(BackendCommand):
    snapshot_name: str
    tag: str

    def execute(self, backend: ZfsBackend) -> None:
        backend.hold_snapshot(self.snapshot_name, self.tag)

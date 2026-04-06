from dataclasses import dataclass
from subprocess import Popen

from ._base import ModelCommand, BackendCommand
from ..model import ZfsModel
from ..backend import ZfsBackend


@dataclass(frozen=True, slots=True)
class ModelSendSnapshot(ModelCommand[Popen]):
    snap_guid: int
    raw: bool
    base_guid: int | None
    include_intermediates: bool
    props: bool
    no_preserve_encryption: bool

    def project(self, model: ZfsModel) -> ZfsModel:
        return model

    def compile(self, model: ZfsModel) -> BackendCommand:
        return BackendSendSnapshot(
            snap=model.snapshots[self.snap_guid].name,
            raw=self.raw,
            base=model.snapshots[self.base_guid].name if self.base_guid is not None else None,
            include_intermediates=self.include_intermediates,
            props=self.props,
            no_preserve_encryption=self.no_preserve_encryption
        )


@dataclass(frozen=True, slots=True)
class BackendSendSnapshot(BackendCommand):
    snap: str
    raw: bool
    base: str | None
    include_intermediates: bool
    props: bool
    no_preserve_encryption: bool

    def execute(self, backend: ZfsBackend) -> None:
        ...

from __future__ import annotations
from abc import ABC, abstractmethod

from ..model import ZfsModel
from ..backend import ZfsBackend


class ModelCommand[T](ABC):
    @abstractmethod
    def compile(self, model: ZfsModel) -> BackendCommand[T]:
        raise NotImplementedError

    @abstractmethod
    def project(self, model: ZfsModel) -> ZfsModel:
        raise NotImplementedError


class BackendCommand[T](ABC):
    """Execution plan against backend primitives."""

    @abstractmethod
    def execute(self, backend: ZfsBackend) -> T:
        raise NotImplementedError

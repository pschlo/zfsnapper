from dataclasses import dataclass

from ..backend import ZfsBackend


class DomainCommand[T]:
    """
    orchestration, cache policy, assembling richer results.

    - May build response from backend model
    - May execute any number of backend commands
    """
    def execute(self, backend: ZfsBackend, *, refresh: bool | None = None) -> T:
        """
        - `refresh`: If True, force backend command execution. If False, never execute backend command (may raise exception). If None, refresh if needed.
        """
        ...


@dataclass
class FullSnapInfo:
    name: str = "dummy"
    guid: int = 0

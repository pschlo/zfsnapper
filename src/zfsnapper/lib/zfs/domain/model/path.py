from __future__ import annotations
from typing import overload, SupportsIndex
from collections.abc import Collection


class PathError(Exception):
    pass


class Path(tuple[str, ...]):
    """A `Path` consists of 0 or more nonempty string segments.
    
    - `segment` = atomic part
    - `component` = one or more segments

    Empty segments are ignored.
    """
    def __new__(cls, *components: Path | str):
        segments = []
        for c in components:
            _segments = c if isinstance(c, Path) else c.split('/')
            segments += [s for s in _segments if s]
        return super().__new__(cls, tuple(segments))

    def __str__(self) -> str:
        return "/".join(self)

    def __repr__(self) -> str:
        return f"Path('{str(self)}')"

    @overload
    def __getitem__(self, key: SupportsIndex) -> str: ...
    @overload
    def __getitem__(self, key: slice) -> Path: ...
    def __getitem__(self, key: SupportsIndex | slice) -> str | Path:
        if isinstance(key, slice):
            # tuple slicing returns tuple[str, ...]; we wrap it back into Path
            return Path(*super().__getitem__(key))
        # keep tuple semantics for indexing; SupportsIndex covers things like numpy ints
        return super().__getitem__(key)
    
    def __truediv__(self, component: Path | str) -> Path:
        return Path(*self, component)

    @property
    def depth(self) -> int:
        """Empty path has depth `0`."""
        return len(self)
    
    def is_ancestor_of(self, other: Path) -> bool:
        """Returns whether `self` is a prefix of `other`."""
        return len(self) <= len(other) and other[:len(self)] == self
    
    def is_descendant_of(self, other: Path) -> bool:
        return other.is_ancestor_of(self)
    
    def relative_to(self, ancestor: Path) -> Path:
        if not ancestor.is_ancestor_of(self):
            raise PathError(f"{self} is not under root {ancestor}")
        return self[len(ancestor):]


EMPTY_PATH: Path = Path()

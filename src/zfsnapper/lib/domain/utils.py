from collections.abc import Collection
from typing import cast, overload, TypeGuard

from .model import Snapshot, Dataset, Pool
from . import typedefs as T

from zfsnapper.common.path import Path


def _normalize_name(name: T.AnySingle) -> str:
    match name:
        case str():
            return name
        case Path():
            return str(name)
        case Snapshot():
            return name.longname
        case Dataset():
            return str(name.path)
        case Pool():
            return name.name
        case _:
            assert False

@overload
def _normalize_names(v: T.AnySingle | T.AnyCollection) -> list[str]: ...
@overload
def _normalize_names(v: T.AnySingle | T.AnyCollection | None) -> list[str] | None: ...
def _normalize_names(v: T.AnySingle | T.AnyCollection | None) -> list[str] | None:
    if v is None:
        return None
    return [_normalize_name(n) for n in _as_container(v)]

def _as_container(v: T.AnySingle | T.AnyCollection) -> T.AnyCollection:
    if isinstance(v, T.AnySingle):
        return cast(T.AnyCollection, [v])
    return v

def _as_snap_container(v: T.Snap | T.Snaps) -> T.Snaps:
    if isinstance(v, T.Snap):
        return cast(T.Snaps, [v])
    return v

def _is_container_type[V](v: T.AnyCollection, typ: type[V]) -> TypeGuard[Collection[V]]:
    try:
        return isinstance(next(iter(v)), typ)
    except TypeError:
        raise ValueError(f"Not a container")
    except StopIteration:
        raise ValueError(f"Cannot determine type of empty container")

def _filter_snaps(snaps: T.Snap | T.Snaps) -> Collection[Snapshot]:
    snaps = _as_snap_container(snaps)
    if _is_container_type(snaps, Snapshot):
        return snaps
    else:
        return []

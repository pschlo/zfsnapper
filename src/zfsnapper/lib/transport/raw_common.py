from typing import overload, Collection


@overload
def _normalize_str(value: str | Collection[str]) -> Collection[str]: ...
@overload
def _normalize_str(value: str | Collection[str] | None) -> Collection[str] | None: ...
def _normalize_str(value: str | Collection[str] | None) -> Collection[str] | None:
    if isinstance(value, str):
        return [value]
    return value

def _is_empty(value: Collection[str] | None) -> bool:
    if value is None:
        return False
    return not bool(value)

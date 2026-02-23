from typing import Callable, Any, TypeVar
from collections.abc import Hashable, Iterable, Sequence, Collection
from itertools import takewhile
from collections import defaultdict


def group_by[Group: Hashable, Item](
    iterable: Iterable[Item],
    key: Callable[[Item], Group],
    ensure_keys: Collection[Group] | None = None
) -> dict[Group, list[Item]]:
    # Identify and fill groups
    groups: dict[Group, list[Item]] = {}
    for item in iterable:
        groups.setdefault(key(item), []).append(item)

    if ensure_keys is not None:
        ensure_set = set(ensure_keys)
        if len(ensure_set) != len(ensure_keys):
            raise ValueError("ensure_keys contains duplicates")

        # Ensure no unexpected keys
        if diff := groups.keys() - ensure_set:
            raise ValueError(f"Unexpected group key: {next(iter(diff))}")

        # Add missing groups as empty
        for g in ensure_set - groups.keys():
            groups[g] = []

        # Put in correct order
        groups = {k: groups[k] for k in ensure_keys}

    return groups


def combine_dicts[K, V1, V2](dict1: dict[K, V1], dict2: dict[K, V2]) -> dict[K, tuple[V1, V2]]:
    """Keys are ordered as in `dict1`."""
    keys = dict1.keys()
    assert dict2.keys() == keys
    return {k: (dict1[k], dict2[k]) for k in keys}


def sort_dict[K, V](dict_: dict[K, V], key: Callable[[K], Any], reverse: bool = False) -> dict[K, V]:
    sorted_keys = sorted(dict_.keys(), key=key, reverse=reverse)
    return {k: dict_[k] for k in sorted_keys}


def space(num: int):
    return " " * (4 * num)

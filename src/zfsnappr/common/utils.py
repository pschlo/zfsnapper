from typing import Callable, Any
from collections.abc import Hashable, Iterable, Sequence
from itertools import takewhile
from collections import defaultdict


def group_by[Group: Hashable, Item](
    iterable: Iterable[Item],
    key: Callable[[Item], Group],
) -> dict[Group, list[Item]]:
    # Identify and fill groups
    groups: dict[Group, list[Item]] = {}
    for item in iterable:
        groups.setdefault(key(item), []).append(item)
    return groups


def combine_dicts[K, V1, V2](dict1: dict[K, V1], dict2: dict[K, V2]) -> dict[K, tuple[V1, V2]]:
    """Keys are ordered as in `dict1`."""
    keys = dict1.keys()
    assert dict2.keys() == keys
    return {k: (dict1[k], dict2[k]) for k in keys}


def sort_dict[K, V](dict_: dict[K, V], key: Callable[[K], Any], reverse: bool = False) -> dict[K, V]:
    sorted_keys = sorted(dict_.keys(), key=key, reverse=reverse)
    return {k: dict_[k] for k in sorted_keys}


def longest_common_prefix[T](*sequences: Sequence[T]) -> tuple[T, ...]:
    cols = zip(*sequences)  # stops at shortest path automatically
    common = (col[0] for col in takewhile(lambda col: all(x == col[0] for x in col), cols))
    return tuple(common)

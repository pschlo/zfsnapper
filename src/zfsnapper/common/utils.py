from typing import Any, overload
from collections.abc import Hashable, Iterable, Sequence, Collection, Callable, Mapping


@overload
def group_by[Group: Hashable, K, V](
    iterable: Mapping[K, V],
    key: Callable[[K], Group],
    ensure_keys: Collection[Group] | None = None,
) -> dict[Group, dict[K, V]]: ...

@overload
def group_by[Group: Hashable, Item](
    iterable: Iterable[Item],
    key: Callable[[Item], Group],
    ensure_keys: Collection[Group] | None = None,
) -> dict[Group, list[Item]]: ...

def group_by[Group: Hashable, Item, V](
    iterable: Iterable[Item] | Mapping[Item, V],
    key: Callable[[Item], Group],
    ensure_keys: Collection[Group] | None = None,
) -> dict[Group, list[Item]] | dict[Group, dict[Item, V]]:
    if ensure_keys is not None:
        ensure_set = set(ensure_keys)
        if len(ensure_set) != len(ensure_keys):
            raise ValueError("ensure_keys contains duplicates")

    if isinstance(iterable, Mapping):
        map_groups: dict[Group, dict[Item, V]] = {}

        for item_key, item_value in iterable.items():
            map_groups.setdefault(key(item_key), {})[item_key] = item_value

        if ensure_keys is not None:
            ensure_set = set(ensure_keys)

            if diff := map_groups.keys() - ensure_set:
                raise ValueError(f"Unexpected group key: {next(iter(diff))}")

            for g in ensure_set - map_groups.keys():
                map_groups[g] = {}

            map_groups = {g: map_groups[g] for g in ensure_keys}

        return map_groups

    list_groups: dict[Group, list[Item]] = {}

    for item in iterable:
        list_groups.setdefault(key(item), []).append(item)

    if ensure_keys is not None:
        ensure_set = set(ensure_keys)

        if diff := list_groups.keys() - ensure_set:
            raise ValueError(f"Unexpected group key: {next(iter(diff))}")

        for g in ensure_set - list_groups.keys():
            list_groups[g] = []

        list_groups = {g: list_groups[g] for g in ensure_keys}

    return list_groups


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


def is_subsequence[T](sub: Sequence[T], master: Sequence[T]) -> bool:
    n = len(sub)
    return any(master[i:i+n] == sub for i in range(len(master) - n + 1))

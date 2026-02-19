from typing import Callable, Any
from collections.abc import Hashable, Iterable


def group_by[Group: Hashable, Item](iterable: Iterable[Item], key: Callable[[Item], Group]) -> dict[Group, list[Item]]:
  groups: dict[Group, list[Item]] = {}
  for item in iterable:
    g = key(item)
    if g not in groups:
      groups[g] = []
    groups[g].append(item)
  return groups


def combine_dicts[K, V1, V2](dict1: dict[K, V1], dict2: dict[K, V2]) -> dict[K, tuple[V1, V2]]:
    keys = dict1.keys()
    assert dict2.keys() == keys
    return {k: (dict1[k], dict2[k]) for k in keys}

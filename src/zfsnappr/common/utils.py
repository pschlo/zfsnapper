from typing import Callable
from collections.abc import Hashable, Iterable


def group_by[Group: Hashable, Item](iterable: Iterable[Item], key: Callable[[Item], Group]) -> dict[Group, list[Item]]:
  groups: dict[Group, list[Item]] = {}
  for item in iterable:
    g = key(item)
    if g not in groups:
      groups[g] = []
    groups[g].append(item)
  return groups

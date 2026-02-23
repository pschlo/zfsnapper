from typing import Optional, Any, overload, Literal
from collections.abc import Callable, Collection
from dataclasses import dataclass

from zfsnappr.common.zfs import Snapshot
from zfsnappr.common.path import Path
from zfsnappr.common.sort import sortkey_dataset
from zfsnappr.common.utils import group_by, sort_dict


@dataclass
class Grouper[G]:
    name: str
    groupkey: Callable[[Snapshot], G]
    sortkey: Callable[[G], Any] | None = None


class groupers:
    DATASET = Grouper[Path](
        name="dataset",
        groupkey=lambda s: s.dataset,
        sortkey=sortkey_dataset
    )
  # TAG = Grouper[int](
  #   name="tag",
  #   groupkey=lambda s: 3,
  #   # sortkey=dataset_sortkey
  # )


def apply_grouper[G](snaps: Collection[Snapshot], grouper: Grouper[G]) -> dict[G, list[Snapshot]]:
    groups = group_by(snaps, grouper.groupkey)
    if grouper.sortkey is not None:
        groups = sort_dict(groups, key=grouper.sortkey)
    return groups

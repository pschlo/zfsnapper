from collections.abc import Collection
from typing import cast

from zfsnappr.common.zfs import Snapshot, Dataset
from .resolve_paths import path_depth


def sort_snaps_by_time(snaps: Collection[Snapshot], reverse: bool = False) -> list[Snapshot]:
    return sorted(
        snaps,
        key=snap_sortkey_by_time,
        reverse=reverse
    )

def snap_sortkey_by_time(snap: Snapshot):
    return (snap.timestamp, dataset_sortkey(snap.dataset), snap.guid)



def sort_datasets(datasets: Collection[Dataset] | Collection[str], reverse: bool = False):
    return sorted(
        datasets,
        key=dataset_sortkey,
        reverse=reverse
    )

def dataset_sortkey(dataset: Dataset | str):
    if isinstance(dataset, str):
        return (path_depth(dataset), dataset)
    return (path_depth(dataset.name), dataset.name)

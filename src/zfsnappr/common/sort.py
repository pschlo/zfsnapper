from collections.abc import Collection
from typing import cast

from zfsnappr.common.zfs import Snapshot, Dataset
from zfsnappr.common.parse_dataset_arg import ConnSpec
from zfsnappr.common.path import Path


def sort_snaps_by_time(snaps: Collection[Snapshot], reverse: bool = False) -> list[Snapshot]:
    return sorted(
        snaps,
        key=snap_sortkey_by_time,
        reverse=reverse
    )

def snap_sortkey_by_time(snap: Snapshot):
    return (snap.timestamp, dataset_sortkey(snap.dataset), snap.guid)



def sort_datasets(datasets: Collection[Dataset] | Collection[Path], reverse: bool = False):
    return sorted(
        datasets,
        key=dataset_sortkey,
        reverse=reverse
    )

def dataset_sortkey(dataset: Dataset | Path | str):
    path = dataset.path if isinstance(dataset, Dataset) else Path(dataset)
    return (path.depth, path)


def sort_conns(conns: Collection[ConnSpec], reverse: bool = False):
    return sorted(
        conns,
        key=conn_sortkey,
        reverse=reverse
    )

def conn_sortkey(conn: ConnSpec):
    return (conn.host, conn.user, conn.port)

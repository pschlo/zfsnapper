from collections.abc import Collection
from typing import cast

from zfsnappr.common.zfs import Snapshot, Dataset
from zfsnappr.common.parse_dataset_arg import ConnSpec
from zfsnappr.common.path import Path


def sortkey_snap_by_time(snap: Snapshot):
    return (snap.timestamp, sortkey_dataset(snap.dataset), snap.guid)

def sortkey_dataset(dataset: Dataset | Path | str):
    path = dataset.path if isinstance(dataset, Dataset) else Path(dataset)
    return path

def sortkey_conn(conn: ConnSpec):
    return (conn.host, conn.user, conn.port)

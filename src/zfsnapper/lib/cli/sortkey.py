from collections.abc import Collection
from typing import cast

from zfsnapper.lib.zfs import Snapshot, Dataset, Path, Peering
from zfsnapper.lib.cli.parse_dataset_arg import ConnSpec


def sortkey_snap_by_time(snap: Snapshot):
    return (snap.timestamp, sortkey_dataset(snap.dataset), snap.guid)

def sortkey_dataset(dataset: Dataset | Path | str):
    path = dataset.path if isinstance(dataset, Dataset) else Path(dataset)
    return path

def sortkey_peering(peering: Peering):
    return (peering.direction, peering.guid)

def sortkey_conn(conn: ConnSpec):
    return (conn.host, conn.user, conn.port)

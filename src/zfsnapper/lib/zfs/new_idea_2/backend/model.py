from __future__ import annotations
from collections.abc import Mapping, Collection
from dataclasses import dataclass, field, replace
from subprocess import Popen
from typing import Any, Literal, TYPE_CHECKING

from .base import LimitedSnapInfo, LimitedDatasetInfo, UniqueSnapKey


DatasetKey = int
SnapshotKey = tuple[Path, int]


@dataclass(frozen=True, slots=True)
class ZfsModel:
    """
    - under each dataset, all snapshot GUIDs must be unique
    - each dataset GUID must be unique
    - thus: when building model
    """
    snapshots: dict[SnapshotKey, LimitedSnapInfo]
    snap_to_key: dict[tuple[Path, str], SnapshotKey]

    datasets: dict[DatasetKey, LimitedDatasetInfo]
    ds_path_to_key: dict[Path, DatasetKey]

    holds: Mapping

    def update_snap(self, snap: LimitedSnapInfo):
        key: SnapshotKey = (snap.dataset, snap.guid)
        self.snapshots[key] = snap
        self.snap_to_key[(snap.dataset, snap.shortname)] = key

    def update_dataset(self, dataset: LimitedDatasetInfo):
        key: DatasetKey = dataset.guid
        self.datasets[key] = dataset
        self.ds_path_to_key[dataset.path] = key


    def find_dataset(self, dataset: int | str | Path) -> LimitedDatasetInfo | None:
        key: DatasetKey | None
        match dataset:
            case int():
                key = dataset
            case str():
                key = self.ds_path_to_key.get(Path(dataset))
            case Path():
                key = self.ds_path_to_key.get(dataset)
            case _:
                assert False

        if key is None:
            return None

        return self.datasets.get(key)


    def find_snapshot(self, dataset: Path, snap: int | str) -> LimitedSnapInfo | None:
        """
        - If dataset given as GUID, must resolve it to string first
        """
        key: SnapshotKey | None
        match snap:
            case int():
                key = (dataset, snap)
            case str():
                key = self.snap_to_key.get((dataset, snap))
            case _:
                assert False
        
        if key is None:
            return None

        return self.snapshots.get(key)


    def find_snapshots(self, datasets: Collection[Path]) -> list[LimitedSnapInfo]:
        for ds in datasets:
            pass
        
        

    # def find_snap_by_key(self, key: UniqueSnapKey) -> LimitedSnapInfo:
    #     # First, find dataset
    #     _key = key.dataset_guid
    #     ds = self.datasets.get(_key)
    #     if ds is None:
    #         raise KeyError()
        
    #     # Then, find snapshot in dataset
    #     _key = (ds.path, key.snapshot_guid)
    #     snap = self.snapshots.get(_key)
    #     if snap is None:
    #         raise KeyError()

    #     return snap

    # def find_snap_by_name(self, dataset: Path, shortname: str) -> LimitedSnapInfo:
    #     # First, find dataset
    #     _matches = [ds for ds in self.datasets.values()]


    # def with_(
    #     self,
    #     *,
    #     snapshots: Mapping[int, LimitedSnapInfo] | None = None,
    #     datasets: Mapping[int, LimitedDatasetInfo] | None = None,
    #     snap_name_to_guid: Mapping[str, int] | None = None,
    #     dataset_name_to_guid: Mapping[str, int] | None = None
    # ):
    #     return ZfsModel(
    #         snapshots=self.snapshots if snapshots is None else snapshots,
    #         datasets=self.datasets if datasets is None else datasets,
    #         snap_name_to_guid=self.snap_name_to_guid if snap_name_to_guid is None else snap_name_to_guid,
    #         dataset_name_to_guid=self.dataset_name_to_guid if dataset_name_to_guid is None else dataset_name_to_guid
    #     )

    # def find_snap
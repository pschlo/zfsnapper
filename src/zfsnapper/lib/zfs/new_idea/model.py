from __future__ import annotations
from dataclasses import dataclass, field
from collections.abc import Mapping, Collection


@dataclass(frozen=True, slots=True)
class ZfsModel:
    snapshots: Mapping[int, SnapshotInfo] = field(default_factory=dict)
    datasets: Mapping[int, DatasetInfo] = field(default_factory=dict)

    snap_name_to_guid: Mapping[str, int] = field(default_factory=dict)
    dataset_name_to_guid: Mapping[str, int] = field(default_factory=dict)

    def with_(
        self,
        *,
        snapshots: Mapping[int, SnapshotInfo] | None = None,
        datasets: Mapping[int, DatasetInfo] | None = None,
        snap_name_to_guid: Mapping[str, int] | None = None,
        dataset_name_to_guid: Mapping[str, int] | None = None
    ):
        return ZfsModel(
            snapshots=self.snapshots if snapshots is None else snapshots,
            datasets=self.datasets if datasets is None else datasets,
            snap_name_to_guid=self.snap_name_to_guid if snap_name_to_guid is None else snap_name_to_guid,
            dataset_name_to_guid=self.dataset_name_to_guid if dataset_name_to_guid is None else dataset_name_to_guid
        )


@dataclass(frozen=True, slots=True)
class SnapshotInfo:
    guid: int
    name: str
    holdtags: frozenset[str]

    def with_(
        self,
        *,
        guid: int | None = None,
        name: str | None = None,
        holdtags: Collection[str] | None = None
    ):
        return SnapshotInfo(
            guid=self.guid if guid is None else guid,
            name=self.name if name is None else name,
            holdtags=self.holdtags if holdtags is None else frozenset(holdtags)
        )


@dataclass
class DatasetInfo:
    guid: int
    name: str

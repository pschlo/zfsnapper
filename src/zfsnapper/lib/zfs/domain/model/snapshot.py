from __future__ import annotations
from dataclasses import dataclass
from collections.abc import Collection
from datetime import datetime

from zfsnapper.lib.zfs import Property, PropertyName, Path

from .peering import Peering


@dataclass(frozen=True)
class SnapshotKey:
    dataset_guid: int
    snapshot_guid: int


@dataclass(frozen=True, eq=False)
class Snapshot:
    dataset: Path
    dataset_guid: int
    shortname: str
    guid: int
    timestamp: datetime
    tags: frozenset[str] | None
    num_holds: int

    properties: dict[str, Property]
    """Properties as fetched from ZFS; may be outdated."""

    holdtags: set[str]

    @property
    def key(self) -> SnapshotKey:
        return SnapshotKey(
            dataset_guid=self.dataset_guid,
            snapshot_guid=self.guid
        )

    @property
    def peerholds(self) -> set[Peering]:
        res: set[Peering] = set()
        for tag in self.holdtags:
            try:
                res.add(Peering.from_tag(tag))
            except ValueError:
                pass
        return res

    def __repr__(self) -> str:
        return f"Snapshot({self.longname})"

    @classmethod
    def from_props(cls, properties: Collection[Property], *, dataset_guid: int):
        P = PropertyName
        ps = {p.propname: p for p in properties}

        dataset_name, shortname = ps[P.NAME].value.split('@')
        dataset = Path(dataset_name)
        guid = int(ps[P.GUID].value)
        timestamp = datetime.fromtimestamp(int(ps[P.CREATION].value))
        num_holds = int(ps[P.USERREFS].value)

        if ps[P.ZFSNAPPER_TAGS].value == '-':
            tags = None
        else:
            tags = frozenset(t for t in ps[P.ZFSNAPPER_TAGS].value.split(',') if t)  # ignore empty tags

        return cls(
            dataset=dataset,
            dataset_guid=dataset_guid,
            shortname=shortname,
            guid=guid,
            timestamp=timestamp,
            tags=tags,
            num_holds=num_holds,
            properties=ps,
            holdtags=set()
        )

    @property
    def longname(self):
        return f'{self.dataset}@{self.shortname}'
    
    def with_dataset(self, dataset: Path | str) -> Snapshot:
        return Snapshot(
            dataset=Path(dataset),
            dataset_guid=self.dataset_guid,
            shortname=self.shortname,
            guid=self.guid,
            timestamp=self.timestamp,
            tags=self.tags,
            num_holds=self.num_holds,
            properties=self.properties,
            holdtags=self.holdtags
        )

    def with_shortname(self, shortname: str) -> Snapshot:
        return Snapshot(
            dataset=self.dataset,
            dataset_guid=self.dataset_guid,
            shortname=shortname,
            guid=self.guid,
            timestamp=self.timestamp,
            tags=self.tags,
            num_holds=self.num_holds,
            properties=self.properties,
            holdtags=self.holdtags
        )
    
    def with_num_holds(self, num_holds: int) -> Snapshot:
        return Snapshot(
            dataset=self.dataset,
            dataset_guid=self.dataset_guid,
            shortname=self.shortname,
            guid=self.guid,
            timestamp=self.timestamp,
            tags=self.tags,
            num_holds=num_holds,
            properties=self.properties,
            holdtags=self.holdtags
        )

    def with_holdtags(self, holdtags: Collection[str]) -> Snapshot:
        return Snapshot(
            dataset=self.dataset,
            dataset_guid=self.dataset_guid,
            shortname=self.shortname,
            guid=self.guid,
            timestamp=self.timestamp,
            tags=self.tags,
            num_holds=self.num_holds,
            properties=self.properties,
            holdtags=set(holdtags)
        )
    
    def with_tags(self, tags: Collection[str]) -> Snapshot:
        return Snapshot(
            dataset=self.dataset,
            dataset_guid=self.dataset_guid,
            shortname=self.shortname,
            guid=self.guid,
            timestamp=self.timestamp,
            tags=frozenset(tags),
            num_holds=self.num_holds,
            properties=self.properties,
            holdtags=self.holdtags
        )

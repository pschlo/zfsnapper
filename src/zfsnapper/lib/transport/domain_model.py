from __future__ import annotations
from dataclasses import dataclass
from collections.abc import Collection
from enum import StrEnum
from datetime import datetime


from .cli_model import Property, ZfsDatasetType, PropertySource
from zfsnapper.common.parse_dataset_arg import ConnSpec
from zfsnapper.common.replication.utils import Direction, Peering
from zfsnapper.common.path import Path


class PropertyName(StrEnum):
    NAME = 'name'
    CREATION = 'creation'
    GUID = 'guid'
    USERREFS = 'userrefs'
    READONLY = 'readonly'
    ATIME = 'atime'
    MOUNTPOINT = 'mountpoint'
    CANMOUNT = 'canmount'
    TYPE = 'type'
    ENCRYPTION = 'encryption'
    ZFSNAPPER_TAGS = 'zfsnapper:tags'  # the user property used to store and read tags
    SHARENFS = 'sharenfs'
    SHARESMB = 'sharesmb'
    QUOTA = 'quota'
    RESERVATION = 'reservation'
    REFRESERVATION = 'refreservation'
    COMPRESSION = 'compression'
    RECORDSIZE = 'recordsize'


@dataclass(eq=False)
class Snapshot:
    dataset: Path
    shortname: str
    guid: int
    timestamp: datetime
    tags: frozenset[str] | None
    num_holds: int

    properties: dict[str, Property]
    """Properties as fetched from ZFS; may be outdated."""

    def __repr__(self) -> str:
        return f"Snapshot({self.longname})"

    @classmethod
    def from_props(cls, properties: Collection[Property]):
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
            shortname=shortname,
            guid=guid,
            timestamp=timestamp,
            tags=tags,
            num_holds=num_holds,
            properties=ps
        )

    @property
    def longname(self):
        return f'{self.dataset}@{self.shortname}'
    
    def with_dataset(self, dataset: Path | str) -> Snapshot:
        return Snapshot(
            dataset=Path(dataset),
            shortname=self.shortname,
            guid=self.guid,
            timestamp=self.timestamp,
            tags=self.tags,
            num_holds=self.num_holds,
            properties=self.properties
        )

    def with_shortname(self, shortname: str) -> Snapshot:
        return Snapshot(
            dataset=self.dataset,
            shortname=shortname,
            guid=self.guid,
            timestamp=self.timestamp,
            tags=self.tags,
            num_holds=self.num_holds,
            properties=self.properties
        )
    
    def with_num_holds(self, num_holds: int) -> Snapshot:
        return Snapshot(
            dataset=self.dataset,
            shortname=self.shortname,
            guid=self.guid,
            timestamp=self.timestamp,
            tags=self.tags,
            num_holds=num_holds,
            properties=self.properties
        )


@dataclass(eq=False)
class Pool:
    name: str
    guid: int

    @classmethod
    def from_props(cls, properties: Collection[Property]):
        P = PropertyName
        ps = {p.propname: p for p in properties}

        name = ps[P.NAME].value
        guid = int(ps[P.GUID].value)

        return Pool(
            name=name,
            guid=guid,
        )


@dataclass(eq=False)
class PeeringInfo:
    peering: Peering
    host: ConnSpec
    path: Path
    pool_guid: int
    last_used: datetime

    @classmethod
    def from_fields(cls, fields: dict[str, str]):
        P = PeerField
        fs = fields
        return PeeringInfo(
            peering=Peering(
                direction=Direction(fs[P.DIRECTION]),
                guid=int(fs[P.GUID])
            ),
            host=ConnSpec.parse(fs[P.HOST]),
            path=Path(fs[P.PATH]),
            pool_guid=int(fs[P.POOL_GUID]),
            last_used=datetime.fromtimestamp(int(fs[P.LAST_USED]))
        )

    def serialize(self, localhost: str | None = None) -> str:
        field_values: dict[PeerField, str] = {
            PeerField.DIRECTION: str(self.peering.direction),
            PeerField.GUID: str(self.peering.guid),
            PeerField.PATH: str(self.path),
            PeerField.HOST: self.host.serialize(localhost=localhost),
            PeerField.POOL_GUID: str(self.pool_guid),
            PeerField.LAST_USED: str(int(self.last_used.timestamp()))
        }
        return ';'.join(f'{f}={v}' for f, v in field_values.items())


@dataclass(eq=False)
class Dataset:
    path: Path
    guid: int
    type: ZfsDatasetType
    is_encrypted: bool
    peerinfos: list[PeeringInfo | None]

    def __repr__(self) -> str:
        return f"Dataset({self.path})"
    
    @property
    def poolname(self) -> str:
        return self.path[0]

    @classmethod
    def from_props(cls, properties: Collection[Property]):
        P = PropertyName
        ps = {p.propname: p for p in properties}

        path = Path(ps[P.NAME].value)
        guid = int(ps[P.GUID].value)
        type = ZfsDatasetType(ps[P.TYPE].value)
        is_encrypted = ps[P.ENCRYPTION].value != 'off'

        # Parse peer slots
        peer_slots_dict: dict[int, PeeringInfo | None] = {}
        for propkey, prop in ps.items():
            parts = propkey.split(':')
            if parts[:2] != ['zfsnapper', 'peer']:
                continue

            slot = int(parts[2])
            # Ignore inherited peer slots
            if prop.value == '-' or prop.source != PropertySource.LOCAL:
                # Slot is empty
                peer_slots_dict[slot] = None
                continue

            # Slot is nonempty
            fields = {}
            for field in prop.value.split(';'):
                f, v = field.split('=', maxsplit=1)
                fields[f] = v
            peer_slots_dict[slot] = PeeringInfo.from_fields(fields)
        
        # Convert peer slots to list.
        # Raises KeyError if slots are not contiguous.
        if not peer_slots_dict:
            peerinfo_slots = []
        else:
            max_slot = max(peer_slots_dict.keys())
            peerinfo_slots = [peer_slots_dict[i] for i in range(max_slot + 1)]

        # Assert no peering is duplicated, i.e. at most 1 info per peering
        _seen: set[Peering] = set()
        for p in peerinfo_slots:
            if p is None:
                continue
            if p.peering in _seen:
                raise ValueError(f"Duplicate peering: {p.peering}")
            _seen.add(p.peering)

        return Dataset(
            path=path,
            guid=guid,
            type=type,
            is_encrypted=is_encrypted,
            peerinfos=peerinfo_slots
        )
    
    def get_peerinfo(self, peering: Peering) -> PeeringInfo | None:
        return next(
            (p for slot, p in enumerate(self.peerinfos) if p is not None and p.peering == peering),
            None
        )
    
    def with_peerinfo_slot(self, slot: int, info: PeeringInfo | None) -> Dataset:
        peerinfos = self.peerinfos
        peerinfos[slot] = info

        return Dataset(
            path=self.path,
            guid=self.guid,
            type=self.type,
            is_encrypted=self.is_encrypted,
            peerinfos=peerinfos
        )


@dataclass(eq=True, frozen=True)
class Hold:
    dataset: Path
    snap_shortname: str
    tag: str

    @property
    def snap_longname(self):
        return f"{self.dataset}@{self.snap_shortname}"



class PeerField(StrEnum):
    """Used for custom user properties of the format `zfsnapper:peer:<slot>:<property>`."""
    DIRECTION = 'direction'
    GUID = 'guid'
    HOST = 'host'
    PATH = 'path'
    POOL_GUID = 'pool_guid'
    LAST_USED = 'last_used'




# properties that will always be fetched
REQUIRED_SNAP_PROPS = [
    PropertyName.NAME,
    PropertyName.CREATION,
    PropertyName.GUID,
    PropertyName.ZFSNAPPER_TAGS,
    PropertyName.USERREFS,
]

REQUIRED_DATASET_PROPS = [
    PropertyName.NAME,
    PropertyName.GUID,
    PropertyName.TYPE,
    PropertyName.ENCRYPTION
]

REQUIRED_POOL_PROPS = [
    PropertyName.NAME,
    PropertyName.GUID
]



PEER_SLOT_PROPERTIES = [f'zfsnapper:peer:{i}' for i in range(50)]

ALL_ZFS_PROPERTIES: list[str] = list(PropertyName) + PEER_SLOT_PROPERTIES

UNEXCLUDABLE_RECEIVE_PROPS = [
    PropertyName.NAME,
    PropertyName.TYPE,
    PropertyName.CREATION,
    PropertyName.USERREFS,
    PropertyName.GUID,
    PropertyName.ENCRYPTION
]
"""Properties which cannot be passed for `zfs receive -x`"""

EXCLUDABLE_RECEIVE_PROPS = set(ALL_ZFS_PROPERTIES) - set(UNEXCLUDABLE_RECEIVE_PROPS)

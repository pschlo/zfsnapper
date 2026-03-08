from __future__ import annotations
from datetime import datetime
from subprocess import Popen, PIPE, CalledProcessError
from typing import Optional, IO, Literal, TYPE_CHECKING
from enum import StrEnum
from collections.abc import Collection
from dataclasses import dataclass
from abc import ABC, abstractmethod
from itertools import batched
import shlex
import logging
from zfsnappr.common.parse_dataset_arg import ConnSpec

from zfsnappr.common.replication.utils import Direction

from .path import Path


log = logging.getLogger(__name__)


class PropertySource(StrEnum):
    NONE = "none"
    INHERITED = "inherited"
    LOCAL = "local"


@dataclass(frozen=True)
class Property:
    propname: str
    value: str
    source: PropertySource

    @classmethod
    def from_raw(cls, property: str, value: str, source: str):
        return Property(
            propname=property,
            value=value,
            source=parse_property_source(source)
        )

def parse_property_source(source: str) -> PropertySource:
    if source == "-":
        return PropertySource.NONE
    if source == "local":
        return PropertySource.LOCAL
    if source.startswith("inherited"):
        return PropertySource.INHERITED
    raise ValueError(f"Invalid property source")


class ZfsProperty:
    NAME = 'name'
    CREATION = 'creation'
    GUID = 'guid'
    USERREFS = 'userrefs'
    READONLY = 'readonly'
    ATIME = 'atime'
    MOUNTPOINT = 'mountpoint'
    CANMOUNT = 'canmount'
    TYPE = 'type'
    CUSTOM_TAGS = 'zfsnappr:tags'  # the user property used to store and read tags


class PeerField(StrEnum):
    """Used for custom user properties of the format `zfsnappr:peer:<slot>:<property>`."""
    DIRECTION = 'direction'
    GUID = 'guid'
    HOST = 'host'
    PATH = 'path'
    POOL_GUID = 'pool_guid'
    LAST_USED = 'last_used'


class ZfsDatasetType(StrEnum):
    FILESYSTEM = 'filesystem'
    VOLUME = 'volume'
    SNAPSHOT = 'snapshot'
    BOOKMARK = 'bookmark'


# properties that will always be fetched
REQUIRED_SNAP_PROPS = [
    ZfsProperty.NAME,
    ZfsProperty.CREATION,
    ZfsProperty.GUID,
    ZfsProperty.CUSTOM_TAGS,
    ZfsProperty.USERREFS,
]

REQUIRED_DATASET_PROPS = [
    ZfsProperty.NAME,
    ZfsProperty.GUID,
    ZfsProperty.TYPE,
]


@dataclass(eq=False)
class Snapshot:
    dataset: Path
    shortname: str
    guid: int
    timestamp: datetime
    tags: frozenset[str] | None
    num_holds: int

    properties: dict[str, str]
    """Properties as fetched from ZFS; may be outdated."""

    def __repr__(self) -> str:
        return f"Snapshot({self.longname})"

    @classmethod
    def from_props(cls, properties: dict[str, str]):
        P = ZfsProperty
        ps = properties

        dataset_name, shortname = ps[P.NAME].split('@')
        dataset = Path(dataset_name)
        guid = int(ps[P.GUID])
        timestamp = datetime.fromtimestamp(int(ps[P.CREATION]))
        num_holds = int(ps[P.USERREFS])

        if ps[P.CUSTOM_TAGS] == '-':
            tags = None
        else:
            tags = frozenset(t for t in ps[P.CUSTOM_TAGS].split(',') if t)  # ignore empty tags

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


@dataclass(eq=False)
class Pool:
    name: str
    guid: int

    @classmethod
    def from_props(cls, properties: Collection[Property]):
        P = ZfsProperty
        ps = {p.propname: p for p in properties}

        name = ps[P.NAME].value
        guid = int(ps[P.GUID].value)

        return Pool(
            name=name,
            guid=guid,
        )


@dataclass(eq=False)
class PeeringInfo:
    direction: Direction
    guid: int
    host: ConnSpec
    path: Path
    pool_guid: int
    last_used: datetime

    @classmethod
    def from_fields(cls, fields: dict[str, str]):
        P = PeerField
        fs = fields
        return PeeringInfo(
            direction=Direction(fs.get(P.DIRECTION, Direction.SEND)),
            guid=int(fs[P.GUID]),
            host=ConnSpec.parse(fs[P.HOST]),
            path=Path(fs[P.PATH]),
            pool_guid=int(fs.get(P.POOL_GUID, 0)),
            last_used=datetime.fromtimestamp(int(fs[P.LAST_USED]))
        )

    def serialize(self) -> str:
        field_values: dict[PeerField, str] = {
            PeerField.DIRECTION: str(self.direction),
            PeerField.GUID: str(self.guid),
            PeerField.PATH: str(self.path),
            PeerField.HOST: self.host.serialize(),
            PeerField.POOL_GUID: str(self.pool_guid),
            PeerField.LAST_USED: str(int(self.last_used.timestamp()))
        }
        return ';'.join(f'{f}={v}' for f, v in field_values.items())


@dataclass(eq=False)
class Dataset:
    path: Path
    guid: int
    type: ZfsDatasetType
    peerinfos: list[PeeringInfo | None]

    def __repr__(self) -> str:
        return f"Dataset({self.path})"
    
    @property
    def poolname(self) -> str:
        return self.path[0]

    @classmethod
    def from_props(cls, properties: Collection[Property]):
        P = ZfsProperty
        ps = {p.propname: p for p in properties}

        path = Path(ps[P.NAME].value)
        guid = int(ps[P.GUID].value)
        type = ZfsDatasetType(ps[P.TYPE].value)

        # Parse peer slots
        peer_slots_dict: dict[int, PeeringInfo | None] = {}
        for propkey, prop in ps.items():
            parts = propkey.split(':')
            if parts[:2] != ['zfsnappr', 'peer']:
                continue

            slot = int(parts[2])
            # Ignore inherited peer slots
            if prop == '-' or prop.source != PropertySource.LOCAL:
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
        max_slot = max(peer_slots_dict.keys())
        peer_slots = [peer_slots_dict[i] for i in range(max_slot+1)]

        # Assert no peer GUID is duplicated
        _guids: set[int] = set()
        for p in peer_slots:
            if p is None:
                continue
            if p.guid in _guids:
                raise ValueError(f"Duplicate peer GUID: {p.guid}")
            _guids.add(p.guid)

        return Dataset(
            path=path,
            guid=guid,
            type=type,
            peerinfos=peer_slots
        )


@dataclass(eq=True, frozen=True)
class Hold:
    dataset: Path
    snap_shortname: str
    tag: str

    @property
    def snap_longname(self):
        return f"{self.dataset}@{self.snap_shortname}"


"""
Each method call should correspond to exactly one CLI call
"""
class ZfsCli(ABC):
    @abstractmethod
    def _start_command(self, cmd: list[str], stdin=None, stdout=None, stderr=None, text=False) -> Popen: ...

    def _run_text_command(self, cmd: list[str]) -> str:
        p: Popen[str] = self._start_command(cmd, stdout=PIPE, text=True)
        stdout, _ = p.communicate()
        if p.returncode > 0:
            raise CalledProcessError(p.returncode, cmd=p.args, output=stdout)
        return stdout

    def send_snapshot_async(self, snapshot_fullname: str, base_fullname: Optional[str] = None) -> Popen[bytes]:
        cmd = ['zfs', 'send', '-v']
        if base_fullname:
            cmd += ['-i', base_fullname]
        cmd += [snapshot_fullname]
        return self._start_command(cmd, stdout=PIPE, stderr=PIPE)

    def receive_snapshot_async(self, dataset: Path | str, stdin: IO[bytes], properties: dict[str, str] = {}) -> Popen[bytes]:
        cmd = ['zfs', 'receive', '-u']
        for property, value in properties.items():
            cmd += ['-o', f'{property}={value}']
        cmd += [str(dataset)]
        return self._start_command(cmd, stdin=stdin)

    # TrueNAS CORE 13.0 does not support holds -p, so we do not fetch timestamp
    def get_holds(self, snapshots_fullnames: Collection[str], userrefs: dict[str, int] | None = None) -> set[Hold]:
        """Optionally pass `userrefs` for performance improvement"""
        if userrefs is not None:
            # Filter snapshots down to those that actually have holds
            snapshots_fullnames = [s for s in snapshots_fullnames if userrefs[s] > 0]
        if not snapshots_fullnames:
            return set()

        holds: set[Hold] = set()
        for batch in batched(snapshots_fullnames, 5000):  # limit how many snapshots can be processed in a single command
            lines = self._run_text_command(['zfs', 'holds', '-H', *batch]).splitlines()
            for line in lines:
                snapname, tag, _ = line.split('\t', 2)
                dataset, shortname = snapname.split('@')
                holds.add(Hold(
                    dataset=Path(dataset),
                    snap_shortname=shortname,
                    tag=tag
                ))
        return holds

    def get_holdtags(self, snapshots_fullnames: Collection[str], userrefs: dict[str, int] | None = None) -> dict[str, set[str]]:
        """Convenience method"""
        holdtags: dict[str, set[str]] = {s: set() for s in snapshots_fullnames}
        for hold in self.get_holds(snapshots_fullnames, userrefs=userrefs):
            holdtags[hold.snap_longname].add(hold.tag)
        return holdtags

    def has_hold(self, snapshot_fullname: str, tag: str) -> bool:
        """Convenience method for checking if snapshot has hold with certain name"""
        return any((s.tag == tag for s in self.get_holds([snapshot_fullname])))

    def hold(self, snapshots_fullnames: Collection[str], tag: str) -> None:
        if not snapshots_fullnames:
            return
        self._run_text_command(['zfs', 'hold', tag, *snapshots_fullnames])

    def release_hold(self, snapshots_fullnames: Collection[str], tag: str) -> None:
        if not snapshots_fullnames:
            return
        self._run_text_command(['zfs', 'release', tag, *snapshots_fullnames])


    def get_pools(self, poolnames: Collection[str] | None = None) -> list[Pool]:
        if poolnames is not None and not poolnames:
            # empty container
            return []
        
        properties = [ZfsProperty.NAME, ZfsProperty.GUID]

        cmd = [
            'zpool', 'get', '-Hp',
            '-o', 'name,property,value,source',
            ','.join(properties)
        ]
        if poolnames is not None:
            cmd += list(poolnames)
        lines = self._run_text_command(cmd).splitlines()

        # Group properties by dataset path
        pool_to_props: dict[str, set[Property]] = {}
        for line in lines:
            name, prop, value, source = line.split('\t')
            pool_to_props.setdefault(name, set()).add(
                Property.from_raw(prop, value, source)
            )

        # Create datasets
        pools = [Pool.from_props(props) for props in pool_to_props.values()]
        return pools

    
    def get_pool(self, poolname: str) -> Pool:
        return next(iter(self.get_pools([poolname])))


    def get_datasets(
        self,
        paths: Collection[Path | str] | None = None,
        properties: Collection[str] = []
    ) -> list[Dataset]:
        if paths is not None and not paths:
            # Empty paths container
            return []
        properties = list(dict.fromkeys(REQUIRED_DATASET_PROPS + list(properties)))  # eliminate duplicates

        # Add peer slots
        properties += [f'zfsnappr:peer:{i}' for i in range(50)]

        cmd: list[str] = [
            'zfs', 'get', '-Hp',
            '-o', 'name,property,value,source',
            '-t', ','.join([ZfsDatasetType.FILESYSTEM, ZfsDatasetType.VOLUME]),
            ','.join(properties)
        ]
        if paths is not None:
            cmd += [str(p) for p in paths]
        lines = self._run_text_command(cmd).splitlines()

        # Group properties by dataset path
        ds_to_props: dict[str, set[Property]] = {}
        for line in lines:
            name, prop, value, source = line.split('\t')
            ds_to_props.setdefault(name, set()).add(
                Property.from_raw(prop, value, source)
            )

        # Create datasets
        datasets = [Dataset.from_props(props) for props in ds_to_props.values()]
        return datasets


    def get_dataset(self, path: Path | str, properties: Collection[str] = []) -> Dataset:
        """Shorthand method"""
        return next(iter(self.get_datasets([path], properties)))

  
    def create_snapshot(self, datasets: Path | str | Collection[Path | str], shortname: str, recursive: bool = False, properties: dict[str, str] = {}) -> None:
        if isinstance(datasets, Path | str):
            datasets = [datasets]
        datasets = [Path(d) for d in datasets]
        if not datasets:
            return

        cmd: list[str] = ['zfs', 'snapshot']
        if recursive:
            cmd += ['-r']
        for property, value in properties.items():
            cmd += ['-o', f'{property}={value}']
        cmd += [f"{d}@{shortname}" for d in datasets]
        self._run_text_command(cmd)
  
    def rename_snapshot(self, fullname: str, new_shortname: str) -> None:
        cmd = ['zfs', 'rename', fullname, new_shortname]
        self._run_text_command(cmd)

    def get_snapshots(self, fullnames: Collection[str], properties: Collection[str] = []) -> list[Snapshot]:
        if not fullnames:
            return []
        properties = list(dict.fromkeys(REQUIRED_SNAP_PROPS + list(properties)))  # eliminate duplicates
        
        cmd = ['zfs', 'get', '-Hp', '-o', 'value', ','.join(properties), *fullnames]
        lines = self._run_text_command(cmd).splitlines()

        snaps: list[Snapshot] = []
        for i in range(len(fullnames)):
            props = {p: v for p, v in zip(properties, lines[i*len(properties):(i+1)*len(properties)])}
            snaps.append(Snapshot.from_props(props))
        return snaps

    def get_all_snapshots(
        self,
        datasets: Collection[Path | str] | None = None,
        recursive: bool = False,
        properties: Collection[str] = [],
    ) -> list[Snapshot]:
        properties = list(dict.fromkeys(REQUIRED_SNAP_PROPS + list(properties)))  # eliminate duplicates
        if datasets is not None and not datasets:
            # empty dataset container
            return []

        cmd = ['zfs', 'list', '-Hp', '-t', 'snapshot', '-o', ','.join(properties)]
        if recursive:
            cmd += ['-r']
        if datasets is not None:
            assert datasets
            cmd += [str(p) for p in datasets]
        lines = self._run_text_command(cmd).splitlines()

        snapshots: list[Snapshot] = []
        for line in lines:
            props = {p: v for p, v in zip(properties, line.split('\t'))}
            snapshots.append(Snapshot.from_props(props))

        return snapshots


    def set_properties(self, objects: Path | str | Collection[Path | str], props_values: dict[str, str]):
        if isinstance(objects, Path | str):
            objects = [objects]
        objects = [str(obj) for obj in objects]
        assert objects

        cmd = ['zfs', 'set']
        cmd += [f'{p}={v}' for p, v in props_values.items()]
        cmd += objects
        self._run_text_command(cmd)

    def set_property(self, objects: Path | str | Collection[Path | str], property: str, value: str):
        self.set_properties(objects, {property: value})

    def unset_property(self, objects: Path | str | Collection[Path | str], property: str):
        if isinstance(objects, Path | str):
            objects = [objects]
        objects = [str(obj) for obj in objects]

        cmd = ['zfs', 'inherit', property]
        cmd += objects
        self._run_text_command(cmd)

    def set_snapshot_tags(self, snap_fullname: str, tags: Collection[str]):
        props = {str(ZfsProperty.CUSTOM_TAGS): ','.join(tags)}
        self.set_properties(snap_fullname, props)

    def destroy_snapshots(self, dataset: Path | str, snapshots_shortnames: Collection[str]) -> None:
        if not snapshots_shortnames:
            return
        shortnames_str = ','.join(snapshots_shortnames)
        self._run_text_command(['zfs', 'destroy', f'{dataset}@{shortnames_str}'])

    def rollback(self, snap_fullname: str) -> None:
        cmd = ['zfs', 'rollback', snap_fullname]
        self._run_text_command(cmd)


class LocalZfsCli(ZfsCli):
    def _start_command(self, cmd: list[str], stdin=None, stdout=None, stderr=None, text=False) -> Popen:
        return Popen(cmd, stdin=stdin, stdout=stdout, stderr=stderr, text=text)


class RemoteZfsCli(ZfsCli):
    ssh_command: list[str]

    def __init__(self, host: str, user: Optional[str], port: Optional[int]) -> None:
        super().__init__()

        cmd = ['ssh']
        if user is not None:
            cmd += ['-l', user]
        if port is not None:
            cmd += ['-p', str(port)]
        cmd += [host]
        self.ssh_command = cmd

    def _start_command(self, cmd: list[str], stdin=None, stdout=None, stderr=None, text=False) -> Popen:
        remote_cmd = ' '.join(shlex.quote(arg) for arg in cmd)
        return Popen(
            self.ssh_command + [remote_cmd],
            stdin=stdin,
            stdout=stdout,
            stderr=stderr,
            text=text
        )

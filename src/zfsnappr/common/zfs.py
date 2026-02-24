from __future__ import annotations
from datetime import datetime
from subprocess import Popen, PIPE, CalledProcessError
from typing import Optional, IO, Literal
from enum import StrEnum
from collections.abc import Collection
from dataclasses import dataclass
from abc import ABC, abstractmethod
from itertools import batched
from enum import StrEnum
import logging

from .path import Path


log = logging.getLogger(__name__)


class ZfsProperty(StrEnum):
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


class PeerProperty(StrEnum):
    """Used for custom user properties of the format `zfsnappr:peer:<slot>:<property>`."""
    GUID = 'guid'
    HOST = 'host'
    PATH = 'path'
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
    holds: int

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
        holds = int(ps[P.USERREFS])

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
            holds=holds,
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
            holds=self.holds,
            properties=self.properties
        )

    def with_shortname(self, shortname: str) -> Snapshot:
        return Snapshot(
            dataset=self.dataset,
            shortname=shortname,
            guid=self.guid,
            timestamp=self.timestamp,
            tags=self.tags,
            holds=self.holds,
            properties=self.properties
        )


@dataclass(eq=False)
class Pool:
    name: str
    guid: int


@dataclass
class Peer:
    guid: int
    host: str
    path: Path
    last_used: datetime

    @classmethod
    def from_props(cls, properties: dict[str, str]):
        P = PeerProperty
        ps = properties
        return Peer(
            guid=int(ps[P.GUID]),
            host=ps[P.HOST],
            path=Path(ps[P.PATH]),
            last_used=datetime.fromtimestamp(int(ps[P.LAST_USED]))
        )


@dataclass(eq=False)
class Dataset:
    path: Path
    guid: int
    type: ZfsDatasetType
    peer_slots: dict[int, Peer | None]

    def __repr__(self) -> str:
        return f"Dataset({self.path})"

    @classmethod
    def from_props(cls, properties: dict[str, str]):
        P = ZfsProperty
        ps = properties

        path = Path(ps[P.NAME])
        guid = int(ps[P.GUID])
        type = ZfsDatasetType(ps[P.TYPE])

        # Parse peer slots
        slot_to_kwargs: dict[int, dict[str, str]] = {}
        for p, v in ps.items():
            parts = p.split(':')
            if parts[:2] != ['zfsnappr', 'peer']:
                continue
            slot = int(parts[2])
            tag = parts[3]

            kwargs = slot_to_kwargs.setdefault(slot, {})
            kwargs[tag] = v

        # Convert to peers
        peers: dict[int, Peer | None] = {}
        for slot, kwargs in slot_to_kwargs.items():
            empty = {k for k, v in kwargs.items() if v == '-'}
            if empty:
                if len(empty) < len(kwargs):
                    # Some keys are empty, some aren't; this is illegal
                    raise RuntimeError(f"Invalid peer properties at slot {slot} of dataset {path}")
                # All are empty
                peers[slot] = None
            else:
                # All are nonempty
                peers[slot] = Peer.from_props(kwargs)

        return Dataset(
            path=path,
            guid=guid,
            type=type,
            peer_slots=peers
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

    def get_pool_from_dataset(self, dataset: Path | str) -> Pool:
        name = str(dataset).split('/')[0]
        guid = self._run_text_command(['zpool', 'get', '-Hp', '-o', 'value', 'guid', name])
        return Pool(name=name, guid=int(guid))
  
    def get_datasets(self, paths: Collection[Path | str], properties: Collection[str] = []) -> list[Dataset]:
        if not paths:
            return []
        properties = list(dict.fromkeys(REQUIRED_DATASET_PROPS + list(properties)))  # eliminate duplicates

        cmd: list[str] = ['zfs', 'get', '-Hp', '-o', 'value', ','.join(properties), *(str(p) for p in paths)]
        lines = self._run_text_command(cmd).splitlines()

        datasets: list[Dataset] = []
        for i in range(len(paths)):
            props = {p: v for p, v in zip(properties, lines[i*len(properties):(i+1)*len(properties)])}
            datasets.append(Dataset.from_props(props))
        return datasets


    def get_dataset(self, path: Path | str, properties: Collection[str] = []) -> Dataset:
        """Shorthand method"""
        return next(iter(self.get_datasets([path], properties)))


    def get_all_datasets(
        self,
        paths: Collection[Path | str] | None = None,
        recursive: bool = False,
        properties: Collection[str] = []
    ) -> list[Dataset]:
        """If `paths` is not `None`, only fetch these datasets."""
        if paths is not None and not paths:
            # Empty paths container
            return []
        properties = list(dict.fromkeys(REQUIRED_DATASET_PROPS + list(properties)))  # eliminate duplicates

        # Add peer properties
        properties += [f'zfsnappr:peer:{i}:{p}' for i in range(30) for p in PeerProperty]

        cmd = ['zfs', 'list', '-Hp', '-o', ','.join(properties)]
        if recursive:
            cmd += ['-r']
        if paths is not None:
            assert paths
            cmd += [str(p) for p in paths]
        lines = self._run_text_command(cmd).splitlines()

        _datasets: list[Dataset] = []
        for line in lines:
            props = {p: v for p, v in zip(properties, line.split('\t'))}
            _datasets.append(Dataset.from_props(props))
    
        return _datasets
  
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


    def set_tags(self, snap_fullname: str, tags: Collection[str]):
        cmd = ['zfs', 'set', f"{ZfsProperty.CUSTOM_TAGS}={','.join(tags)}", snap_fullname]
        self._run_text_command(cmd)

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
        cmd = self.ssh_command + cmd
        return Popen(cmd, stdin=stdin, stdout=stdout, stderr=stderr, text=text)

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

from zfsnapper.common.parse_dataset_arg import ConnSpec
from zfsnapper.common.replication.utils import Direction, Peering

from .path import Path


log = logging.getLogger(__name__)


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

    def send_snapshot_async(self, snapshot_fullname: str, raw: bool, base_fullname: Optional[str] = None, include_intermediates: bool = False, props: bool = False, no_preserve_encryption: bool = False) -> Popen[bytes]:
        if include_intermediates and base_fullname is None:
            raise ValueError("include_intermediates=True requires a base snapshot")

        cmd = ['zfs', 'send', '-v']
        if raw:
            cmd += ['-w']
        if props:
            cmd += ['-p']
        if no_preserve_encryption:
            cmd += ['-U']
        if base_fullname:
            cmd += [
                '-I' if include_intermediates else '-i',
                base_fullname
            ]
        cmd += [snapshot_fullname]
        return self._start_command(cmd, stdout=PIPE, stderr=PIPE)

    def receive_snapshot_async(self, dataset: Path | str, stdin: IO[bytes], override_props: dict[str, str] = {}, exclude_props: Collection[str] = []) -> Popen[bytes]:
        cmd = ['zfs', 'receive', '-u']
        for property, value in override_props.items():
            cmd += ['-o', f'{property}={value}']
        for property in exclude_props:
            cmd += ['-x', f'{property}']
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

    def hold(self, snapshots_fullnames: str | Collection[str], tag: str) -> None:
        if isinstance(snapshots_fullnames, str):
            snapshots_fullnames = [snapshots_fullnames]
        if not snapshots_fullnames:
            return
        self._run_text_command(['zfs', 'hold', tag, *snapshots_fullnames])

    def release_hold(self, snapshots_fullnames: str | Collection[str], tag: str) -> None:
        if isinstance(snapshots_fullnames, str):
            snapshots_fullnames = [snapshots_fullnames]
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
        properties += PEER_SLOT_PROPERTIES

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
        if not objects:
            return

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
        if not objects:
            return

        cmd = ['zfs', 'inherit', property]
        cmd += objects
        self._run_text_command(cmd)

    def set_snapshot_tags(self, snap_fullnames: str | Collection[str], tags: Collection[str]):
        if isinstance(snap_fullnames, str):
            snap_fullnames = [snap_fullnames]

        props = {str(ZfsProperty.ZFSNAPPER_TAGS): ','.join(tags)}
        self.set_properties(snap_fullnames, props)

    def destroy_snapshots(self, dataset: Path | str, snapshots_shortnames: str | Collection[str]) -> None:
        if isinstance(snapshots_shortnames, str):
            snapshots_shortnames = [snapshots_shortnames]
        if not snapshots_shortnames:
            return

        shortnames_str = ','.join(snapshots_shortnames)
        self._run_text_command(['zfs', 'destroy', f'{dataset}@{shortnames_str}'])

    def rollback(self, snap_fullname: str) -> None:
        cmd = ['zfs', 'rollback', snap_fullname]
        self._run_text_command(cmd)


class LocalZfsCli(ZfsCli):
    def _start_command(self, cmd: list[str], stdin=None, stdout=None, stderr=None, text=False) -> Popen:
        log.debug(f"Running local command: {' '.join(cmd)}")
        return Popen(cmd, stdin=stdin, stdout=stdout, stderr=stderr, text=text)


class RemoteZfsCli(ZfsCli):
    ssh_command: list[str]

    def __init__(self, host: str, user: Optional[str], port: Optional[int]) -> None:
        super().__init__()

        cmd = [
            "ssh",
            "-o", "ControlMaster=auto",
            "-o", "ControlPersist=5m",
            "-o", "ControlPath=~/.ssh/cm-%C",
        ]
        if user is not None:
            cmd += ['-l', user]
        if port is not None:
            cmd += ['-p', str(port)]
        cmd += [host]
        self.ssh_command = cmd

    def _start_command(self, cmd: list[str], stdin=None, stdout=None, stderr=None, text=False) -> Popen:
        log.debug(f"Running ssh command: {' '.join(cmd)}")
        remote_cmd = ' '.join(shlex.quote(arg) for arg in cmd)
        return Popen(
            self.ssh_command + [remote_cmd],
            stdin=stdin,
            stdout=stdout,
            stderr=stderr,
            text=text
        )

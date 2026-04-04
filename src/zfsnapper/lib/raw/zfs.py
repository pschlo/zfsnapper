from subprocess import Popen, PIPE
from collections.abc import Collection
from typing import IO

from ..transport import CommandRunner
from .model import Property, RawHold
from zfsnapper.common.utils import group_by

from .common import _is_empty, _normalize_str


class RawZfs:
    def __init__(self, runner: CommandRunner):
        self._runner = runner

    def send_async(
        self,
        snapshot_fullname: str,
        raw: bool,
        base_fullname: str | None = None,
        include_intermediates: bool = False,
        props: bool = False,
        no_preserve_encryption: bool = False
    ) -> Popen[bytes]:
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
        return self._runner.start_command(cmd, stdout=PIPE, stderr=PIPE)

    def receive_async(
        self,
        dataset: str,
        stdin: IO[bytes],
        override_props: dict[str, str] = {},
        exclude_props: Collection[str] = []
    ) -> Popen[bytes]:
        cmd = ['zfs', 'receive', '-u']
        for property, value in override_props.items():
            cmd += ['-o', f'{property}={value}']
        for property in exclude_props:
            cmd += ['-x', f'{property}']
        cmd += [dataset]
        return self._runner.start_command(cmd, stdin=stdin)

    def get(
        self,
        properties: str | Collection[str],
        targets: str | Collection[str] | None = None,
        *,
        types: str | Collection[str] | None = None,
        recursive: bool = False
    ) -> list[Property]:
        properties = _normalize_str(properties)
        targets = _normalize_str(targets)
        types = _normalize_str(types)
        if _is_empty(properties):
            raise ValueError("properties must not be empty")
        if _is_empty(targets):
            return []
        if _is_empty(types):
            return []

        cmd: list[str] = [
            'zfs', 'get', '-Hp',
            '-o', 'name,property,value,source',
        ]
        if recursive:
            cmd += ['-r']
        if types:
            cmd += ['-t', ','.join(types)]
        cmd += [','.join(properties)]
        if targets:
            cmd += list(targets)

        lines = self._runner.run_text_command(cmd).splitlines()
        props: list[Property] = []
        for line in lines:
            name, property, value, source = line.split('\t')
            props.append(Property.from_raw(name, property, value, source))
        return props

    def holds(self, snapshots_fullnames: str | Collection[str]) -> list[RawHold]:
        snapshots_fullnames = _normalize_str(snapshots_fullnames)
        if _is_empty(snapshots_fullnames):
            return []

        lines = self._runner.run_text_command(['zfs', 'holds', '-H', *snapshots_fullnames]).splitlines()
        holds: list[RawHold] = []
        for line in lines:
            name, tag, timestamp = line.split('\t')
            holds.append(RawHold.from_raw(name, tag))
        return holds

    def hold(self, snapshots_fullnames: str | Collection[str], tag: str) -> None:
        snapshots_fullnames = _normalize_str(snapshots_fullnames)
        if _is_empty(snapshots_fullnames):
            return

        self._runner.run_text_command(['zfs', 'hold', tag, *snapshots_fullnames])

    def release(self, snapshots_fullnames: str | Collection[str], tag: str) -> None:
        snapshots_fullnames = _normalize_str(snapshots_fullnames)
        if _is_empty(snapshots_fullnames):
            return

        self._runner.run_text_command(['zfs', 'release', tag, *snapshots_fullnames])

    def snapshot(
        self,
        datasets: str | Collection[str],
        shortname: str,
        recursive: bool = False,
        properties: dict[str, str] = {}
    ) -> None:
        datasets = _normalize_str(datasets)
        if _is_empty(datasets):
            return

        cmd: list[str] = ['zfs', 'snapshot']
        if recursive:
            cmd += ['-r']
        for property, value in properties.items():
            cmd += ['-o', f'{property}={value}']
        cmd += [f"{d}@{shortname}" for d in datasets]
        self._runner.run_text_command(cmd)

    def rename(self, fullname: str, new_shortname: str) -> None:
        cmd = ['zfs', 'rename', fullname, new_shortname]
        self._runner.run_text_command(cmd)

    def set(self, objects: str | Collection[str], props_values: dict[str, str]) -> None:
        objects = _normalize_str(objects)
        if _is_empty(objects):
            return

        cmd = ['zfs', 'set']
        cmd += [f'{p}={v}' for p, v in props_values.items()]
        cmd += objects
        self._runner.run_text_command(cmd)

    def inherit(self, objects: str | Collection[str], property: str) -> None:
        objects = _normalize_str(objects)
        if _is_empty(objects):
            return

        cmd = ['zfs', 'inherit', property]
        cmd += objects
        self._runner.run_text_command(cmd)

    def destroy(self, snap_longnames: str | Collection[str]) -> None:
        snap_longnames = _normalize_str(snap_longnames)
        if _is_empty(snap_longnames):
            return
        
        ds_to_snaps = group_by(snap_longnames, key=lambda s: s.split('@')[0])
        if len(ds_to_snaps) > 1:
            raise ValueError(f"All snapshots must be of same datasets")
        dataset = next(iter(ds_to_snaps.keys()))
        shortnames = [s.split('@')[1] for s in snap_longnames]

        shortnames_str = ','.join(shortnames)
        self._runner.run_text_command(['zfs', 'destroy', f'{dataset}@{shortnames_str}'])

    def rollback(self, snap_fullname: str) -> None:
        cmd = ['zfs', 'rollback', snap_fullname]
        self._runner.run_text_command(cmd)

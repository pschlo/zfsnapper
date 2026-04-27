from __future__ import annotations
from dataclasses import dataclass, field, replace
from collections.abc import Collection
from enum import StrEnum
from subprocess import Popen, PIPE
from typing import Any, Literal, IO, overload

from zfsnapper.lib.cli.utils import group_by

from .runner import CommandRunner
from .model import ZfsModel
from .base import BackendCommand, LimitedSnapInfo, LimitedDatasetInfo


@dataclass(frozen=True)
class Property:
    objname: str
    propname: str
    value: str
    source: PropertySource

    @classmethod
    def from_raw(cls, name: str, property: str, value: str, source: str):
        return Property(
            objname=name,
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
    if source == "default":
        return PropertySource.DEFAULT
    raise ValueError(f"Invalid property source")


class PropertySource(StrEnum):
    NONE = "none"
    INHERITED = "inherited"
    DEFAULT = "default"
    LOCAL = "local"


@overload
def _normalize_str(value: str | Collection[str]) -> Collection[str]: ...
@overload
def _normalize_str(value: str | Collection[str] | None) -> Collection[str] | None: ...
def _normalize_str(value: str | Collection[str] | None) -> Collection[str] | None:
    if isinstance(value, str):
        return [value]
    return value

def _is_empty(value: Collection[str] | None) -> bool:
    if value is None:
        return False
    return not bool(value)



@dataclass
class _Get(BackendCommand[list[Property]]):
    properties: str | Collection[str]
    targets: str | Collection[str] | None = None
    types: str | Collection[str] | None = None
    recursive: bool = False

    def execute(self, runner: CommandRunner) -> list[Property]:
        properties = _normalize_str(self.properties)
        targets = _normalize_str(self.targets)
        types = _normalize_str(self.types)
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
        if self.recursive:
            cmd += ['-r']
        if types:
            cmd += ['-t', ','.join(types)]
        cmd += [','.join(properties)]
        if targets:
            cmd += list(targets)

        lines = runner.run_text_command(cmd).splitlines()
        props: list[Property] = []
        for line in lines:
            name, property, value, source = line.split('\t')
            props.append(Property.from_raw(name, property, value, source))
        return props
    
    def project(self, result: list[Property], model: ZfsModel) -> None:
        pass


@dataclass
class GetSnapshots(BackendCommand[list[LimitedSnapInfo]]):
    extra_props: str | Collection[str] = []
    datasets: str | Collection[str] | None = None
    recursive: bool = False

    def execute(self, runner: CommandRunner):
        # Inject required properties
        props = [*REQUIRED_SNAP_PROPS, *_normalize_str(self.extra_props)]

        fetched_props = _Get(
            properties=props,
            targets=self.datasets,
            types=ZfsDatasetType.SNAPSHOT,
            recursive=self.recursive
        ).execute(runner)

        snapname_to_props = group_by(fetched_props, key=lambda p: p.objname)

        snap_infos = [
            LimitedSnapInfo.from_props(props)
            for snapname, props in snapname_to_props.items()
        ]
        return snap_infos

    def project(self, result, model) -> None:
        for snap in result:
            model.update_snap(snap)


@dataclass
class GetDatasets(BackendCommand[list[LimitedDatasetInfo]]):
    extra_props: str | Collection[str]
    datasets: str | Collection[str] | None = None
    recursive: bool = False

    def execute(self, runner: CommandRunner):
        # Inject required properties
        props = [*REQUIRED_DATASET_PROPS, *_normalize_str(self.extra_props)]

        fetched_props = _Get(
            properties=props,
            targets=self.datasets,
            types=[ZfsDatasetType.FILESYSTEM, ZfsDatasetType.VOLUME],
            recursive=self.recursive
        ).execute(runner)

        dsname_to_props = group_by(fetched_props, key=lambda p: p.objname)

        ds_infos = [
            LimitedDatasetInfo.from_props(props)
            for dsname, props in dsname_to_props.items()
        ]
        return ds_infos

    def project(self, result, model) -> None:
        for ds in result:
            model.update_dataset(ds)


@dataclass
class SendAsync(BackendCommand[Popen[bytes]]):
    snapshot_fullname: str
    raw: bool
    base_fullname: str | None = None
    include_intermediates: bool = False
    props: bool = False
    no_preserve_encryption: bool = False

    def execute(self, runner: CommandRunner):
        if self.include_intermediates and self.base_fullname is None:
            raise ValueError("include_intermediates=True requires a base snapshot")

        cmd = ['zfs', 'send', '-v']
        if self.raw:
            cmd += ['-w']
        if self.props:
            cmd += ['-p']
        if self.no_preserve_encryption:
            cmd += ['-U']
        if self.base_fullname:
            cmd += [
                '-I' if self.include_intermediates else '-i',
                self.base_fullname
            ]
        cmd += [self.snapshot_fullname]
        return runner.start_command(cmd, stdout=PIPE, stderr=PIPE)

    def project(self, result, model) -> None:
        pass


@dataclass
class ReceiveAsync(BackendCommand[Popen[bytes]]):
    dataset: str
    stdin: IO[bytes]
    override_props: dict[str, str] = {}
    exclude_props: Collection[str] = []

    def execute(self, runner: CommandRunner) -> Popen[bytes]:
        cmd = ['zfs', 'receive', '-u']
        for property, value in self.override_props.items():
            cmd += ['-o', f'{property}={value}']
        for property in self.exclude_props:
            cmd += ['-x', f'{property}']
        cmd += [self.dataset]
        return runner.start_command(cmd, stdin=self.stdin)

    def project(self, result: Popen[bytes], model: ZfsModel) -> None:
        pass

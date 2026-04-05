from __future__ import annotations
from dataclasses import dataclass
from typing import cast, overload, IO
from collections.abc import Collection
from subprocess import Popen

from zfsnapper.lib.zfs.domain import Pool, Dataset, Snapshot, ZfsCli, Peering, Hold
from zfsnapper.lib.zfs.transport import SshCommandRunner, LocalCommandRunner
from zfsnapper.lib.cli.parse_dataset_arg import ConnSpec
from zfsnapper.lib.cli.resolve_datasets import create_zfs_cli


class SnapshotRef:
    guid: int
    session: ZfsSession

    def resolve(self) -> Snapshot:
        return self.session._registry.get_snapshot(self.guid)

    @property
    def num_holds(self):
        return self.resolve().num_holds

    @property
    def holdtags(self):
        return self.resolve().holdtags


class DatasetRef:
    guid: int
    session: ZfsSession

    def resolve(self) -> Dataset:
        return self.session._registry.get_dataset(self.guid)

    @property
    def is_encrypted(self):
        return self.resolve().is_encrypted

    @property
    def path(self):
        return self.resolve().path
    
    @property
    def type(self):
        return self.resolve().type

    @property
    def peerinfos(self):
        return self.resolve().peerinfos



class Registry:
    _pools: dict[int, Pool]
    _datasets: dict[int, Dataset]
    _snapshots: dict[int, Snapshot]

    def __init__(self) -> None:
        self._pools = {}
        self._datasets = {}
        self._snapshots = {}


    def get_pool(self, guid: int) -> Pool:
        return self._pools[guid]

    def get_dataset(self, guid: int) -> Dataset:
        return self._datasets[guid]
    
    def get_snapshot(self, guid: int) -> Snapshot:
        return self._snapshots[guid]


    def update(self, objs: Pool | Dataset | Snapshot | Collection[Pool | Dataset | Snapshot]):
        objs = _as_container(objs)
        for obj in objs:
            match obj:
                case Pool():
                    self._pools[obj.guid] = obj
                case Dataset():
                    self._datasets[obj.guid] = obj
                case Snapshot():
                    self._snapshots[obj.guid] = obj
                case _:
                    assert False

    def remove(self, objs: Pool | Dataset | Snapshot | Collection[Pool | Dataset | Snapshot]):
        objs = _as_container(objs)
        for obj in objs:
            match obj:
                case Pool():
                    self._pools.pop(obj.guid, None)
                case Dataset():
                    self._datasets.pop(obj.guid, None)
                case Snapshot():
                    self._snapshots.pop(obj.guid, None)
                case _:
                    assert False


class ZfsSession:
    """
    - A single ZFS instance/host.
    - Defined by a ConnSpec
    - consists of one or more pools
    - stateful
    """
    _cli: ZfsCli
    _registry: Registry

    def __init__(self, cli: ZfsCli, registry: Registry) -> None:
        self._cli = cli
        self._registry = registry

    @classmethod
    def from_connspec(cls, conn: ConnSpec):
        cli = create_zfs_cli(conn)
        return cls(
            cli=cli,
            registry=Registry()
        )


    def send_snapshot_async(
        self,
        snap: Snapshot,
        raw: bool,
        base: Snapshot | None = None,
        include_intermediates: bool = False,
        props: bool = False,
        no_preserve_encryption: bool = False
    ) -> Popen[bytes]:
        return self._cli.send_snapshot_async(
            snap=snap,
            raw=raw,
            base=base,
            include_intermediates=include_intermediates,
            props=props,
            no_preserve_encryption=no_preserve_encryption
        )


    def receive_snapshot_async(
        self,
        dataset: Dataset,
        stdin: IO[bytes],
        override_props: dict[str, str] = {},
        exclude_props: Collection[str] = []
    ) -> Popen[bytes]:
        return self._cli.receive_snapshot_async(
            dataset=dataset,
            stdin=stdin,
            override_props=override_props,
            exclude_props=exclude_props
        )


    def get_holds(self, snaps: Snapshot | Collection[Snapshot]) -> set[Hold]:
        return self._cli.get_holds(
            snaps=snaps
        )


    @overload
    def get_holdtags(self, snaps: Collection[str]) -> dict[str, set[str]]: ...
    @overload
    def get_holdtags(self, snaps: Collection[Snapshot]) -> dict[Snapshot, set[str]]: ...
    @overload
    def get_holdtags(self, snaps: str | Snapshot) -> set[str]: ...
    def get_holdtags(self, snaps: str | Snapshot | Collection[str] | Collection[Snapshot]):
        return self._cli.get_holdtags(
            snaps=snaps
        )


    def add_hold(self, snaps: Snapshot | Collection[Snapshot], tag: str | Peering):
        updated_snaps = self._cli.add_hold(
            snaps=snaps,
            tag=tag
        )
        self._registry.update(updated_snaps)


    def release_hold(self, snaps: Snapshot | Collection[Snapshot], tag: str | Peering):
        updated_snaps = self._cli.release_hold(
            snaps=snaps,
            tag=tag
        )
        self._registry.update(updated_snaps)


    def remove_peer(self, dataset: Dataset, peering: Peering):
        updated_dataset, updated_snaps = self._cli.remove_peer(
            dataset=dataset,
            peering=peering,
            snaps=self._registry._snapshots.values(),  # TODO
        )
        self._registry.update(updated_dataset)
        self._registry.update(updated_snaps)


    def get_pool(
        self,
        poolname: Pool,
        properties: Collection[str] = []
    ) -> Pool:
        pool = self._cli.get_pool(
            poolname=poolname,
            properties=properties
        )
        self._registry.update(pool)
        return pool


    def get_pools(
        self,
        poolnames: Pool | Collection[Pool] | None = None,
        properties: Collection[str] = []
    ) -> list[Pool]:
        pools = self._cli.get_pools(
            poolnames=poolnames,
            properties=properties
        )
        self._registry.update(pools)
        return pools


    def get_dataset(
        self,
        path: Dataset,
        properties: Collection[str] = [],
    ) -> Dataset:
        dataset = self._cli.get_dataset(
            path=path,
            properties=properties
        )
        self._registry.update(dataset)
        return dataset


    def get_datasets(
        self,
        paths: Dataset | Collection[Dataset] | None = None,
        properties: Collection[str] = [],
        recursive: bool = False
    ) -> list[Dataset]:
        datasets = self._cli.get_datasets(
            paths=paths,
            properties=properties,
            recursive=recursive
        )
        self._registry.update(datasets)
        return datasets


    def create_snapshot(
        self,
        datasets: Dataset | Collection[Dataset],
        shortname: str,
        recursive: bool = False,
        properties: dict[str, str] = {}
    ) -> None:
        return self._cli.create_snapshot(
            datasets=datasets,
            shortname=shortname,
            recursive=recursive,
            properties=properties
        )


    def rename_snapshot(self, snap: Snapshot, new_shortname: str):
        updated_snap = self._cli.rename_snapshot(
            snap=snap,
            new_shortname=new_shortname
        )
        self._registry.update(updated_snap)


    def get_snapshots(
        self,
        datasets: Dataset | Collection[Dataset] | None = None,
        properties: Collection[str] = [],
        recursive: bool = False,
        holdtags: bool = False
    ) -> list[Snapshot]:
        snaps = self._cli.get_snapshots(
            datasets=datasets,
            properties=properties,
            recursive=recursive,
            holdtags=holdtags
        )
        self._registry.update(snaps)
        return snaps


    def set_snapshot_tags(self, snaps: Snapshot | Collection[Snapshot], tags: Collection[str]) -> None:
        updated_snaps = self._cli.set_snapshot_tags(
            snaps=snaps,
            tags=tags
        )
        self._registry.update(updated_snaps)

    def destroy_snapshots(self, snaps: Snapshot | Collection[Snapshot]) -> None:
        self._cli.destroy_snapshots(
            snaps=snaps
        )
        self._registry.remove(snaps)

    def rollback(self, snap: Snapshot) -> None:
        return self._cli.rollback(
            snap=snap
        )



AnySingle = Snapshot | Dataset | Pool
AnyCollection = Collection[Snapshot] | Collection[Dataset] | Collection[Pool]

@overload
def _as_container(v: Snapshot | Collection[Snapshot]) -> Collection[Snapshot]: ...
@overload
def _as_container(v: Dataset | Collection[Dataset]) -> Collection[Dataset]: ...
@overload
def _as_container(v: Pool | Collection[Pool]) -> Collection[Pool]: ...
@overload
def _as_container(v: Pool | Dataset | Snapshot | Collection[Pool | Dataset | Snapshot]) -> Collection[Pool | Dataset | Snapshot]: ...
def _as_container(v):
    if isinstance(v, AnySingle):
        return cast(AnyCollection, [v])
    return v

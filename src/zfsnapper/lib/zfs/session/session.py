from __future__ import annotations

from dataclasses import dataclass
from typing import IO, overload, cast
from collections.abc import Collection, Iterable
from subprocess import Popen

from zfsnapper.lib.zfs.domain import Pool, Dataset, Snapshot, ZfsCli, Peering, Hold, SnapshotKey
from zfsnapper.lib.cli.parse_dataset_arg import ConnSpec
from zfsnapper.lib.cli.resolve_datasets import create_zfs_cli


AnyObj = Pool | Dataset | Snapshot
AnyObjCollection = Collection[Pool | Dataset | Snapshot]


class Registry:
    def __init__(self) -> None:
        self._pools: dict[int, Pool] = {}
        self._datasets: dict[int, Dataset] = {}
        self._snapshots: dict[SnapshotKey, Snapshot] = {}

    def update(self, objs: AnyObj | AnyObjCollection | None) -> None:
        if objs is None:
            return

        for obj in _as_container(objs):
            match obj:
                case Pool():
                    self._pools[obj.guid] = obj
                case Dataset():
                    self._datasets[obj.guid] = obj
                case Snapshot():
                    self._snapshots[obj.key] = obj
                case _:
                    raise TypeError(f"Unsupported object type: {type(obj)!r}")

    def remove(self, objs: AnyObj | AnyObjCollection | None) -> None:
        if objs is None:
            return

        for obj in _as_container(objs):
            match obj:
                case Pool():
                    self._pools.pop(obj.guid, None)
                case Dataset():
                    self._datasets.pop(obj.guid, None)
                case Snapshot():
                    self._snapshots.pop(obj.key, None)
                case _:
                    raise TypeError(f"Unsupported object type: {type(obj)!r}")

    def pool(self, guid: int) -> Pool:
        return self._pools[guid]

    def dataset(self, guid: int) -> Dataset:
        return self._datasets[guid]

    def snapshot(self, key: SnapshotKey) -> Snapshot:
        return self._snapshots[key]

    def pools(self) -> list[Pool]:
        return list(self._pools.values())

    def datasets(self) -> list[Dataset]:
        return list(self._datasets.values())

    def snapshots(self) -> list[Snapshot]:
        return list(self._snapshots.values())

    def snapshots_for_dataset(self, dataset: Dataset) -> list[Snapshot]:
        return [s for s in self._snapshots.values() if s.dataset == dataset.path]

    def has_snapshot(self, guid: int) -> bool:
        return guid in self._snapshots

    def has_dataset(self, guid: int) -> bool:
        return guid in self._datasets

    def has_pool(self, guid: int) -> bool:
        return guid in self._pools


@dataclass(frozen=True)
class SnapshotRef:
    session: ZfsSession
    key: SnapshotKey

    def resolve(self) -> Snapshot:
        return self.session.snapshot(self.key)

    def refresh(self) -> Snapshot:
        return self.session.fetch_snapshot_by_key(self.key)

    def add_hold(self, tag: str | Peering) -> Snapshot:
        return self.session.add_hold(self, tag)

    def release_hold(self, tag: str | Peering) -> Snapshot:
        return self.session.release_hold(self, tag)

    def destroy(self) -> None:
        self.session.destroy_snapshots(self)

    @property
    def num_holds(self) -> int:
        return self.resolve().num_holds

    @property
    def holdtags(self):
        return self.resolve().holdtags

    @property
    def timestamp(self):
        return self.resolve().timestamp

    @property
    def longname(self) -> str:
        return self.resolve().longname


@dataclass(frozen=True)
class DatasetRef:
    session: ZfsSession
    guid: int

    def resolve(self) -> Dataset:
        return self.session.dataset(self.guid)

    def refresh(self) -> Dataset:
        return self.session.fetch_dataset_by_guid(self.guid)

    def snapshots(self) -> list[SnapshotRef]:
        return self.session.snapshot_refs(self)

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


@dataclass(frozen=True)
class PoolRef:
    session: ZfsSession
    guid: int

    def resolve(self) -> Pool:
        return self.session.pool(self.guid)

    def refresh(self) -> Pool:
        return self.session.fetch_pool_by_guid(self.guid)


class ZfsSession:
    """
    A stateful session for one ZFS host / connection.
    Owns:
    - the ZfsCli transport/domain facade
    - the canonical in-memory registry
    """

    def __init__(self, cli: ZfsCli, registry: Registry | None = None) -> None:
        self._cli = cli
        self._registry = registry or Registry()

    @classmethod
    def from_connspec(cls, conn: ConnSpec) -> ZfsSession:
        return cls(cli=create_zfs_cli(conn))

    # ----------------------------
    # Registry-only accessors
    # ----------------------------

    def pool(self, guid: int) -> Pool:
        return self._registry.pool(guid)

    def dataset(self, guid: int) -> Dataset:
        return self._registry.dataset(guid)

    def snapshot(self, key: SnapshotKey) -> Snapshot:
        return self._registry.snapshot(key)

    def pools(self) -> list[Pool]:
        return self._registry.pools()

    def datasets(self) -> list[Dataset]:
        return self._registry.datasets()

    def snapshots(self, dataset: Dataset | DatasetRef | None = None) -> list[Snapshot]:
        if dataset is None:
            return self._registry.snapshots()

        ds = _resolve_dataset(dataset)
        return self._registry.snapshots_for_dataset(ds)

    def pool_ref(self, guid: int) -> PoolRef:
        return PoolRef(session=self, guid=guid)

    def dataset_ref(self, guid: int) -> DatasetRef:
        return DatasetRef(session=self, guid=guid)

    def snapshot_ref(self, key: SnapshotKey) -> SnapshotRef:
        return SnapshotRef(session=self, key=key)

    def pool_refs(self) -> list[PoolRef]:
        return [PoolRef(self, p.guid) for p in self.pools()]

    def dataset_refs(self) -> list[DatasetRef]:
        return [DatasetRef(self, d.guid) for d in self.datasets()]

    def snapshot_refs(self, dataset: Dataset | DatasetRef | None = None) -> list[SnapshotRef]:
        return [SnapshotRef(self, s.key) for s in self.snapshots(dataset)]


    # ----------------------------
    # Live fetch methods
    # ----------------------------

    def fetch_pool(
        self,
        poolname,
        properties: Collection[str] = [],
    ) -> Pool:
        pool = self._cli.get_pool(
            poolname=poolname,
            properties=properties,
        )
        self._registry.update(pool)
        return pool

    def fetch_pools(
        self,
        poolnames=None,
        properties: Collection[str] = [],
    ) -> list[Pool]:
        pools = self._cli.get_pools(
            poolnames=poolnames,
            properties=properties,
        )
        self._registry.update(pools)
        return pools

    def fetch_dataset(
        self,
        path,
        properties: Collection[str] = [],
    ) -> Dataset:
        dataset = self._cli.get_dataset(
            path=path,
            properties=properties,
        )
        self._registry.update(dataset)
        return dataset

    def fetch_datasets(
        self,
        paths=None,
        properties: Collection[str] = [],
        recursive: bool = False,
    ) -> list[Dataset]:
        datasets = self._cli.get_datasets(
            paths=paths,
            properties=properties,
            recursive=recursive,
        )
        self._registry.update(datasets)
        return datasets

    def fetch_snapshots(
        self,
        datasets=None,
        properties: Collection[str] = [],
        recursive: bool = False,
        holdtags: bool = False,
    ) -> list[Snapshot]:
        """
        - datasets may be given in any way
        - we try to find match and fetch missing datasets
        - we then get snapshots
        """
        snaps = self._cli.get_snapshots(
            datasets=datasets,
            properties=properties,
            recursive=recursive,
            fetch_holdtags=holdtags,
            fetch_parents=False
        )
        self._registry.update(snaps)
        return snaps

    def fetch_pool_by_guid(self, guid: int) -> Pool:
        current = self.pool(guid)
        return self.fetch_pool(current.name)

    def fetch_dataset_by_guid(self, guid: int) -> Dataset:
        current = self.dataset(guid)
        return self.fetch_dataset(current.path)

    def fetch_snapshot_by_key(self, key: SnapshotKey) -> Snapshot:
        current = self.snapshot(key)
        snaps = self.fetch_snapshots(
            datasets=[current.dataset],
            recursive=False,
            holdtags=True,
        )
        refreshed = next((s for s in snaps if s.key == key), None)
        if refreshed is None:
            raise KeyError(f"Snapshot with key={key} no longer exists")
        return refreshed

    # ----------------------------
    # Non-mutating / passthrough-ish operations
    # ----------------------------

    def send_snapshot_async(
        self,
        snap: Snapshot | SnapshotRef,
        raw: bool,
        base: Snapshot | SnapshotRef | None = None,
        include_intermediates: bool = False,
        props: bool = False,
        no_preserve_encryption: bool = False,
    ) -> Popen[bytes]:
        resolved_snap = _resolve_snapshot(snap)
        resolved_base = None if base is None else _resolve_snapshot(base)
        return self._cli.send_snapshot_async(
            snap=resolved_snap,
            raw=raw,
            base=resolved_base,
            include_intermediates=include_intermediates,
            props=props,
            no_preserve_encryption=no_preserve_encryption,
        )

    def receive_snapshot_async(
        self,
        dataset: Dataset | DatasetRef,
        stdin: IO[bytes],
        override_props: dict[str, str] | None = None,
        exclude_props: Collection[str] | None = None,
    ) -> Popen[bytes]:
        return self._cli.receive_snapshot_async(
            dataset=_resolve_dataset(dataset),
            stdin=stdin,
            override_props=dict(override_props or {}),
            exclude_props=list(exclude_props or []),
        )

    def get_holds(self, snaps: Snapshot | SnapshotRef | Collection[Snapshot] | Collection[SnapshotRef]) -> set[Hold]:
        resolved = _resolve_snapshots(snaps)
        return self._cli.get_holds(snaps=resolved)

    @overload
    def get_holdtags(self, snaps: Collection[str]) -> dict[str, set[str]]: ...
    @overload
    def get_holdtags(self, snaps: Collection[Snapshot]) -> dict[Snapshot, set[str]]: ...
    @overload
    def get_holdtags(self, snaps: str | Snapshot) -> set[str]: ...
    def get_holdtags(self, snaps):
        return self._cli.get_holdtags(snaps=snaps)

    # ----------------------------
    # Mutating operations
    # These update the registry after success.
    # ----------------------------

    @overload
    def add_hold(self, snaps: SnapshotRef, tag: str | Peering) -> Snapshot: ...
    @overload
    def add_hold(self, snaps: Snapshot, tag: str | Peering) -> Snapshot: ...
    @overload
    def add_hold(self, snaps: Collection[SnapshotRef], tag: str | Peering) -> list[Snapshot]: ...
    @overload
    def add_hold(self, snaps: Collection[Snapshot], tag: str | Peering) -> list[Snapshot]: ...
    def add_hold(self, snaps, tag):
        resolved = _resolve_snapshots(snaps)
        updated = self._cli.add_hold(snaps=resolved, tag=tag)
        self._registry.update(updated)
        return updated

    @overload
    def release_hold(self, snaps: SnapshotRef, tag: str | Peering) -> Snapshot: ...
    @overload
    def release_hold(self, snaps: Snapshot, tag: str | Peering) -> Snapshot: ...
    @overload
    def release_hold(self, snaps: Collection[SnapshotRef], tag: str | Peering) -> list[Snapshot]: ...
    @overload
    def release_hold(self, snaps: Collection[Snapshot], tag: str | Peering) -> list[Snapshot]: ...
    def release_hold(self, snaps, tag):
        resolved = _resolve_snapshots(snaps)
        updated = self._cli.release_hold(snaps=resolved, tag=tag)
        self._registry.update(updated)
        return updated

    def remove_peer(self, dataset: Dataset | DatasetRef, peering: Peering) -> tuple[Dataset, list[Snapshot]]:
        ds = _resolve_dataset(dataset)

        snaps = self.snapshots(ds)
        if not snaps:
            snaps = self.fetch_snapshots(ds, holdtags=True)

        updated_dataset, updated_snaps = self._cli.remove_peer(
            dataset=ds,
            peering=peering,
            snaps=snaps,
        )
        self._registry.update(updated_dataset)
        self._registry.update(updated_snaps)
        return updated_dataset, updated_snaps

    def create_snapshot(
        self,
        datasets: Dataset | DatasetRef | Collection[Dataset] | Collection[DatasetRef],
        shortname: str,
        recursive: bool = False,
        properties: dict[str, str] | None = None,
    ) -> None:
        resolved = _resolve_datasets(datasets)
        self._cli.create_snapshot(
            datasets=resolved,
            shortname=shortname,
            recursive=recursive,
            properties=dict(properties or {}),
        )

    def rename_snapshot(self, snap: Snapshot | SnapshotRef, new_shortname: str) -> Snapshot:
        updated = self._cli.rename_snapshot(
            snap=_resolve_snapshot(snap),
            new_shortname=new_shortname,
        )
        self._registry.update(updated)
        return updated

    def set_snapshot_tags(
        self,
        snaps: Snapshot | SnapshotRef | Collection[Snapshot] | Collection[SnapshotRef],
        tags: Collection[str],
    ):
        resolved = _resolve_snapshots(snaps)
        updated = self._cli.set_snapshot_tags(
            snaps=resolved,
            tags=tags,
        )
        self._registry.update(updated)
        return updated

    def destroy_snapshots(
        self,
        snaps: Snapshot | SnapshotRef | Collection[Snapshot] | Collection[SnapshotRef],
    ) -> None:
        resolved = _resolve_snapshots(snaps)
        self._cli.destroy_snapshots(snaps=resolved)
        self._registry.remove(resolved)

    def rollback(self, snap: Snapshot | SnapshotRef) -> None:
        self._cli.rollback(snap=_resolve_snapshot(snap))



@overload
def _as_container(v: Pool) -> Collection[Pool]: ...
@overload
def _as_container(v: Dataset) -> Collection[Dataset]: ...
@overload
def _as_container(v: Snapshot) -> Collection[Snapshot]: ...
@overload
def _as_container(v: AnyObjCollection) -> AnyObjCollection: ...
def _as_container(v):
    if isinstance(v, Pool | Dataset | Snapshot):
        return [v]
    return v


def _resolve_snapshot(v: Snapshot | SnapshotRef) -> Snapshot:
    return v.resolve() if isinstance(v, SnapshotRef) else v


def _resolve_dataset(v: Dataset | DatasetRef) -> Dataset:
    return v.resolve() if isinstance(v, DatasetRef) else v


def _resolve_snapshots(
    v: Snapshot | SnapshotRef | Collection[Snapshot] | Collection[SnapshotRef],
) -> Snapshot | list[Snapshot]:
    if isinstance(v, Snapshot | SnapshotRef):
        return _resolve_snapshot(v)
    return [_resolve_snapshot(s) for s in v]


def _resolve_datasets(
    v: Dataset | DatasetRef | Collection[Dataset] | Collection[DatasetRef],
) -> Dataset | list[Dataset]:
    if isinstance(v, Dataset | DatasetRef):
        return _resolve_dataset(v)
    return [_resolve_dataset(d) for d in v]

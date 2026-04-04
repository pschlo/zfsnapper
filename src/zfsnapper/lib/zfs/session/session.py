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
        return self.session.snapshot(self.guid)

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
        return self.session.dataset(self.guid)

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


class ZfsSession:
    """
    - A single ZFS instance/host.
    - Defined by a ConnSpec
    - consists of one or more pools
    - stateful
    """
    _cli: ZfsCli
    _pools: dict[int, Pool]
    _datasets: dict[int, Dataset]
    _snapshots: dict[int, Snapshot]

    def __init__(self, cli: ZfsCli) -> None:
        self._cli = cli

    @classmethod
    def from_connspec(cls, conn: ConnSpec):
        cli = create_zfs_cli(conn)
        return cls(
            cli=cli
        )

    def snapshot(self, guid: int) -> Snapshot:
        return self._snapshots[guid]

    def update_snapshots(self, snaps: Snapshot | Collection[Snapshot]) -> None:
        snaps = _as_container(snaps)
        for snap in snaps:
            self._snapshots[snap.guid] = snap

    def dataset(self, guid: int) -> Dataset:
        return self._datasets[guid]

    def update_datasets(self, datasets: Dataset | Collection[Dataset]) -> None:
        datasets = _as_container(datasets)
        for ds in datasets:
            self._datasets[ds.guid] = ds


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
        self.update_snapshots(updated_snaps)


    def release_hold(self, snaps: Snapshot | Collection[Snapshot], tag: str | Peering):
        updated_snaps = self._cli.release_hold(
            snaps=snaps,
            tag=tag
        )
        self.update_snapshots(updated_snaps)


    def remove_peer(self, dataset: Dataset, peering: Peering):
        updated_dataset, updated_snaps = self._cli.remove_peer(
            dataset=dataset,
            peering=peering,
            snaps=self._snapshots.values(),  # TODO
        )
        self.update_datasets(updated_dataset)
        self.update_snapshots(updated_snaps)


    def get_pool(
        self,
        poolname: Pool,
        properties: Collection[str] = []
    ) -> Pool:
        return self._cli.get_pool(
            poolname=poolname,
            properties=properties
        )
    

    def get_pools(
        self,
        poolnames: Pool | Collection[Pool] | None = None,
        properties: Collection[str] = []
    ) -> list[Pool]:
        return self._cli.get_pools(
            poolnames=poolnames,
            properties=properties
        )


    def get_dataset(
        self,
        path: Dataset,
        properties: Collection[str] = [],
    ) -> Dataset:
        return self._cli.get_dataset(
            path=path,
            properties=properties
        )


    def get_datasets(
        self,
        paths: Dataset | Collection[Dataset] | None = None,
        properties: Collection[str] = [],
        recursive: bool = False
    ) -> list[Dataset]:
        return self._cli.get_datasets(
            paths=paths,
            properties=properties,
            recursive=recursive
        )


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
        self.update_snapshots(updated_snap)


    def get_snapshots(
        self,
        datasets: Dataset | Collection[Dataset] | None = None,
        properties: Collection[str] = [],
        recursive: bool = False,
        holdtags: bool = False
    ) -> list[Snapshot]:
        return self._cli.get_snapshots(
            datasets=datasets,
            properties=properties,
            recursive=recursive,
            holdtags=holdtags
        )


    def set_snapshot_tags(self, snaps: Snapshot | Collection[Snapshot], tags: Collection[str]) -> None:
        updated_snaps = self._cli.set_snapshot_tags(
            snaps=snaps,
            tags=tags
        )
        self.update_snapshots(updated_snaps)

    def destroy_snapshots(self, snaps: Snapshot | Collection[Snapshot]) -> None:
        self._cli.destroy_snapshots(
            snaps=snaps
        )
        # TODO: remove the snap from registry

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
def _as_container(v: AnySingle | AnyCollection) -> AnyCollection:
    if isinstance(v, AnySingle):
        return cast(AnyCollection, [v])
    return v

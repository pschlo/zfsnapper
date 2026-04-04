from collections.abc import Collection
from subprocess import Popen
from itertools import batched
import logging
from typing import IO, cast, overload

from zfsnapper.lib.zfs.raw import RawZfs, RawZpool, ZfsDatasetType, RawHold
from .model import Snapshot, Dataset, Hold, Pool, REQUIRED_DATASET_PROPS, REQUIRED_SNAP_PROPS, REQUIRED_POOL_PROPS, PropertyName, PeeringInfo, Peering
from .utils import _normalize_name, _normalize_names, _as_snap_container, _filter_snaps, _is_container_type, _is_container
from . import typedefs as T

from zfsnapper.lib.cli.utils import group_by, space
from zfsnapper.lib.zfs import Path

log = logging.getLogger(__name__)


class ZfsCli:
    def __init__(self, raw_zfs: RawZfs, raw_zpool: RawZpool):
        self._zfs = raw_zfs
        self._zpool = raw_zpool

    def send_snapshot_async(
        self,
        snapshot_fullname: str,
        raw: bool,
        base_fullname: str | None = None,
        include_intermediates: bool = False,
        props: bool = False,
        no_preserve_encryption: bool = False
    ) -> Popen[bytes]:
        return self._zfs.send_async(
            snapshot_fullname=snapshot_fullname,
            raw=raw,
            base_fullname=base_fullname,
            include_intermediates=include_intermediates,
            props=props,
            no_preserve_encryption=no_preserve_encryption
        )

    def receive_snapshot_async(
        self,
        dataset: T.Dataset,
        stdin: IO[bytes],
        override_props: dict[str, str] = {},
        exclude_props: Collection[str] = []
    ) -> Popen[bytes]:
        return self._zfs.receive_async(
            dataset=_normalize_name(dataset),
            stdin=stdin,
            override_props=override_props,
            exclude_props=exclude_props
        )

    def get_holds(self, snaps: T.Snap | T.Snaps) -> set[Hold]:
        # Filter snapshots down to those that actually have holds
        snaps = _as_snap_container(snaps)
        if _is_container_type(snaps, Snapshot):
            snaps = [s for s in snaps if s.num_holds > 0]

        _raw_holds: list[RawHold] = []
        for batch in batched(snaps, 5000):  # arbitrary limit how many snapshots can be processed in a single command
            batch = cast(T.Snaps, batch)
            _raw_holds += self._zfs.holds(_normalize_names(batch))

        holds: set[Hold] = set()
        for h in _raw_holds:
            dataset, shortname = h.snap_longname.split('@')
            holds.add(Hold(
                dataset=Path(dataset),
                snap_shortname=shortname,
                tag=h.tag
            ))
        return holds

    @overload
    def with_holdtags(self, snaps: Snapshot) -> Snapshot: ...
    @overload
    def with_holdtags(self, snaps: Collection[Snapshot]) -> list[Snapshot]: ...
    def with_holdtags(self, snaps: Snapshot | Collection[Snapshot]) -> Snapshot | list[Snapshot]:
        if isinstance(snaps, Snapshot):
            tags = self.get_holdtags(snaps)
            return snaps.with_holdtags(tags)

        tags = self.get_holdtags(snaps)
        return [s.with_holdtags(tags[s]) for s in snaps]


    @overload
    def get_holdtags(self, snaps: Collection[str]) -> dict[str, set[str]]: ...
    @overload
    def get_holdtags(self, snaps: Collection[Snapshot]) -> dict[Snapshot, set[str]]: ...
    @overload
    def get_holdtags(self, snaps: T.Snap) -> set[str]: ...
    def get_holdtags(self, snaps: T.Snap | T.Snaps):
        holds = self.get_holds(snaps)
        if isinstance(snaps, T.Snap):
            tags = {h.tag for h in holds}
            return tags
        elif _is_container_type(snaps, str):
            snaps = cast(Collection[str], snaps)
            snapname_to_holds = group_by(holds, lambda h: h.snap_longname, ensure_keys=snaps)
            tags = {s: {h.tag for h in holds} for s, holds in snapname_to_holds.items()}
            return {s: tags[s] for s in snaps}
        else:
            assert _is_container_type(snaps, Snapshot)
            snapname_to_holds = group_by(holds, lambda h: h.snap_longname, ensure_keys=[s.longname for s in snaps])
            tags = {s: {h.tag for h in holds} for s, holds in snapname_to_holds.items()}
            return {s: tags[s.longname] for s in snaps}


    @overload
    def add_hold(self, snaps: str, tag: str | Peering) -> None: ...
    @overload
    def add_hold(self, snaps: Snapshot, tag: str | Peering) -> Snapshot: ...
    @overload
    def add_hold(self, snaps: Collection[str], tag: str | Peering) -> None: ...
    @overload
    def add_hold(self, snaps: Collection[Snapshot], tag: str | Peering) -> list[Snapshot]: ...
    def add_hold(self, snaps: T.Snap | T.Snaps, tag: str | Peering) -> Snapshot | list[Snapshot] | None:
        if isinstance(tag, Peering):
            tag = tag.to_tag()

        self._zfs.hold(
            snapshots_fullnames=_normalize_names(snaps),
            tag=tag
        )

        # If snaps were given as strings, return None
        snaps_container = _as_snap_container(snaps)
        if _is_container_type(snaps_container, str):
            return None

        # Determine new snap objects
        assert _is_container_type(snaps_container, Snapshot)
        new_snaps = []
        for s in snaps_container:
            if tag in s.holdtags:
                # Nothing changed
                continue
            new_snap = (
                s
                .with_num_holds(s.num_holds + 1)
                .with_holdtags(s.holdtags | {tag})
            )
            new_snaps.append(new_snap)

        if isinstance(snaps, Snapshot):
            assert len(new_snaps) == 1
            return next(iter(new_snaps))
        return new_snaps


    @overload
    def release_hold(self, snaps: str, tag: str | Peering) -> None: ...
    @overload
    def release_hold(self, snaps: Snapshot, tag: str | Peering) -> Snapshot: ...
    @overload
    def release_hold(self, snaps: Collection[str], tag: str | Peering) -> None: ...
    @overload
    def release_hold(self, snaps: Collection[Snapshot], tag: str | Peering) -> list[Snapshot]: ...
    def release_hold(self, snaps: T.Snap | T.Snaps, tag: str | Peering) -> Snapshot | list[Snapshot] | None:
        if isinstance(tag, Peering):
            tag = tag.to_tag()

        self._zfs.release(
            snapshots_fullnames=_normalize_names(snaps),
            tag=tag
        )

        # If snaps were given as strings, return None
        snaps_container = _as_snap_container(snaps)
        if _is_container_type(snaps_container, str):
            return None

        # Determine new snap objects
        assert _is_container_type(snaps_container, Snapshot)
        new_snaps = []
        for s in snaps_container:
            new_snap = (
                s
                .with_num_holds(s.num_holds - 1)
                .with_holdtags(s.holdtags - {tag})
            )
            new_snaps.append(new_snap)

        if isinstance(snaps, Snapshot):
            assert len(new_snaps) == 1
            return next(iter(new_snaps))
        return new_snaps

    def get_pool(
        self,
        poolname: T.Pool,
        properties: Collection[str] = []
    ) -> Pool:
        pools = self.get_pools(
            poolnames=poolname,
            properties=properties
        )
        assert len(pools) == 1
        return next(iter(pools))

    def get_pools(
        self,
        poolnames: T.Pool | T.Pools | None = None,
        properties: Collection[str] = []
    ) -> list[Pool]:
        # Inject required properties
        properties = [*REQUIRED_POOL_PROPS, *properties]

        fetched_props = self._zpool.get(
            properties=properties,
            targets=_normalize_names(poolnames)
        )
        pool_to_props = group_by(fetched_props, key=lambda p: p.objname)
        pools = [Pool.from_props(props) for props in pool_to_props.values()]
        return pools

    def get_dataset(
        self,
        path: T.Dataset,
        properties: Collection[str] = [],
    ) -> Dataset:
        datasets = self.get_datasets(
            paths=path,
            properties=properties,
            recursive=False
        )
        assert len(datasets) == 1
        return next(iter(datasets))

    def get_datasets(
        self,
        paths: T.Dataset | T.Datasets | None = None,
        properties: Collection[str] = [],
        recursive: bool = False
    ) -> list[Dataset]:
        # Inject required properties
        properties = [*REQUIRED_DATASET_PROPS, *properties]

        fetched_props = self._zfs.get(
            properties=properties,
            targets=_normalize_names(paths),
            types=[ZfsDatasetType.FILESYSTEM, ZfsDatasetType.VOLUME],
            recursive=recursive
        )
        ds_to_props = group_by(fetched_props, key=lambda p: p.objname)
        datasets = [Dataset.from_props(props) for props in ds_to_props.values()]
        return datasets

    def create_snapshot(
        self,
        datasets: T.Dataset | T.Datasets,
        shortname: str,
        recursive: bool = False,
        properties: dict[str, str] = {}
    ) -> None:
        self._zfs.snapshot(
            datasets=_normalize_names(datasets),
            shortname=shortname,
            recursive=recursive,
            properties=properties
        )

    def rename_snapshot(self, snap: T.Snap, new_shortname: str) -> None:
        return self._zfs.rename(
            fullname=_normalize_name(snap),
            new_shortname=new_shortname
        )

    def get_snapshots(
        self,
        datasets: T.Dataset | T.Datasets | None = None,
        properties: Collection[str] = [],
        recursive: bool = False,
        holdtags: bool = False
    ) -> list[Snapshot]:
        # Inject required properties
        properties = [*REQUIRED_SNAP_PROPS, *properties]

        fetched_props = self._zfs.get(
            properties=properties,
            targets=_normalize_names(datasets),
            types=[ZfsDatasetType.SNAPSHOT],
            recursive=recursive,
        )
        snap_to_props = group_by(fetched_props, key=lambda p: p.objname)
        snaps = [Snapshot.from_props(props) for props in snap_to_props.values()]

        if holdtags:
            snaps = self.with_holdtags(snaps)

        return snaps

    def set_properties(self, objects: T.Snap | T.Snaps | T.Dataset | T.Datasets, props_values: dict[str, str]) -> None:
        self._zfs.set(
            objects=_normalize_names(objects),
            props_values=props_values
        )

    def set_property(self, objects: T.Snap | T.Snaps | T.Dataset | T.Datasets, property: str, value: str) -> None:
        self.set_properties(
            objects=objects,
            props_values={property: value}
        )

    def unset_property(self, objects: T.Snap | T.Snaps | T.Dataset | T.Datasets, property: str) -> None:
        self._zfs.inherit(
            objects=_normalize_names(objects),
            property=property
        )

    def set_snapshot_tags(self, snaps: T.Snap | T.Snaps, tags: Collection[str]) -> None:
        props = {str(PropertyName.ZFSNAPPER_TAGS): ','.join(tags)}
        self.set_properties(snaps, props)

    def destroy_snapshots(self, snaps: T.Snap | T.Snaps) -> None:
        self._zfs.destroy(
            snap_longnames=_normalize_names(snaps)
        )

    def rollback(self, snap: T.Snap) -> None:
        self._zfs.rollback(
            snap_fullname=_normalize_name(snap)
        )


    ###### Peer methods #######

    def _set_peerinfo_slot(
        self,
        dataset: Dataset,
        peer: PeeringInfo,
        slot: int,
        localhost: str | None = None
    ) -> Dataset:
        """Serializes the peer and stores it at the given slot on the dataset."""
        self.set_property(dataset, f"zfsnapper:peer:{slot}", peer.serialize(localhost=localhost))
        return dataset.with_peerinfo_slot(slot, peer)


    def _clear_peerinfo_slot(
        self,
        dataset: Dataset,
        slot: int
    ) -> Dataset:
        self.unset_property(dataset, f'zfsnapper:peer:{slot}')
        return dataset.with_peerinfo_slot(slot, None)


    def update_peerinfo(
        self,
        dataset: Dataset,
        peerinfo: PeeringInfo,
        localhost: str | None = None
    ) -> Dataset:
        """Update peer if it already exists, else add under first free slot."""
        # Find peer GUID
        curr_slot = next(
            (slot for slot, p in enumerate(dataset.peerinfos) if p is not None and p.peering == peerinfo.peering),
            None
        )
        if curr_slot is not None:
            # Peer already exists in slot; overwrite
            return self._set_peerinfo_slot(dataset=dataset, peer=peerinfo, slot=curr_slot, localhost=localhost)

        # Find first free slot
        slot = next((slot for slot, p in enumerate(dataset.peerinfos) if p is None), None)
        if slot is None:
            raise RuntimeError(f"Cannot set peer on dataset {dataset.path}: no free slots")
        return self._set_peerinfo_slot(dataset=dataset, peer=peerinfo, slot=slot, localhost=localhost)


    def remove_peer(
        self,
        dataset: Dataset,
        peering: Peering,
        *,
        snaps: Collection[Snapshot],
        log_indent: int = 0
    ) -> Dataset:
        """Removes peer from dataset.
        
        Removes both PeerInfo and holds of peer."""
        def _s(i: int = 0):
            return space(log_indent+i)

        # Try to find in PeerInfos
        r = next(
            ((slot, p) for slot, p in enumerate(dataset.peerinfos) if p and p.peering == peering),
            None
        )
        if r is not None:
            # Clear slot
            slot, peer = r
            dataset = self._clear_peerinfo_slot(dataset=dataset, slot=slot)

        # Determine peer holds on that dataset
        held_snaps = [s for s in snaps if s.dataset == dataset.path and peering in s.peerholds]
        log.debug(_s() + f"Removing {len(held_snaps)} obsolete holds")
        self.release_hold(held_snaps, peering)

        return dataset

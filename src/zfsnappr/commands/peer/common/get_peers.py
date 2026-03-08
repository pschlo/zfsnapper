from __future__ import annotations
from collections.abc import Collection

from zfsnappr.common.zfs import Dataset, Snapshot
from zfsnappr.common.resolve_datasets import ResolvedDatasets
from zfsnappr.common.command_utils import parse_holdtags, Path, group_by, Peering


def get_peers(snaps: Collection[Snapshot], holds: dict[Snapshot, set[str]], datasets: ResolvedDatasets):
    """
    - How many peers does a given dataset have?
    - How many holds does a given peer on a dataset have?
    """
    _ds_to_snaps = group_by(snaps, key=lambda s: s.dataset, ensure_keys=datasets.p.matched)

    _ds_to_holds: dict[
        Dataset,
        set[tuple[Snapshot, Peering]]
    ] = {}
    for dset in datasets.matched:
        _holds: set[tuple[Snapshot, Peering]] = set()
        for s in _ds_to_snaps[dset.path]:
            _holds.update((s, h) for h in parse_holdtags(holds[s]))
        _ds_to_holds[dset] = _holds


    # Peerings on datasets plus peerings on holds
    ds_to_peerings: dict[Dataset, set[Peering]] = {}
    for ds, _holds in _ds_to_holds.items():
        ds_to_peerings[ds] = {
            *(peering for _, peering in _holds),
            *(p.peering for p in ds.peerinfos if p)
        }


    # Group holds by (ds, peering)
    ds_peer_to_holds = {(d.path, p): set[Snapshot]() for d, ps in ds_to_peerings.items() for p in ps}
    for ds, _holds in _ds_to_holds.items():
        for snap, h in _holds:
            ds_peer_to_holds[(ds.path, h)].add(snap)

    return ds_to_peerings, ds_peer_to_holds

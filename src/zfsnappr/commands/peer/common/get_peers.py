from __future__ import annotations
from collections.abc import Collection

from zfsnappr.common.zfs import Dataset, Snapshot
from zfsnappr.common.resolve_datasets import ResolvedDatasets
from zfsnappr.common.command_utils import parse_holdtags, Path, group_by, ReplicationHold


def get_peers(snaps: Collection[Snapshot], holds: dict[Snapshot, set[str]], datasets: ResolvedDatasets):
    """
    - How many peers does a given dataset have?
    - How many holds does a given peer on a dataset have?
    """
    _ds_to_snaps = group_by(snaps, key=lambda s: s.dataset, ensure_keys=datasets.p.matched)

    _ds_to_holds: dict[
        Dataset,
        set[tuple[Snapshot, ReplicationHold]]
    ] = {}
    for dset in datasets.matched:
        _holds: set[tuple[Snapshot, ReplicationHold]] = set()
        for s in _ds_to_snaps[dset.path]:
            _holds.update((s, h) for h in parse_holdtags(holds[s]))
        _ds_to_holds[dset] = _holds


    # Registered GUIDs PLUS those on holds
    ds_to_peers: dict[Dataset, set[int]] = {}
    for ds, _holds in _ds_to_holds.items():
        ds_to_peers[ds] = {
            *(h.guid for _, h in _holds),
            *(p.guid for p in ds.peerinfos if p)
        }


    # Group holds by (ds, peer)
    ds_peer_to_holds: dict[
        tuple[Path, int],
        set[tuple[Snapshot, ReplicationHold]]
    ] = {}
    for ds, _holds in _ds_to_holds.items():
        for snap, h in _holds:
            ds_peer_to_holds.setdefault((ds.path, h.guid), set()).add((snap, h))
    
    # Update keys so that all (ds, peer) keys are present
    for d, ps in ds_to_peers.items():
        for p in ps:
            ds_peer_to_holds.setdefault((d.path, p), set())

    return ds_to_peers, ds_peer_to_holds

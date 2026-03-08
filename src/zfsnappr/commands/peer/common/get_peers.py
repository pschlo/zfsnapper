from __future__ import annotations
from typing import cast, Optional, TYPE_CHECKING
import logging
from collections.abc import Collection

from zfsnappr.common.zfs import ZfsProperty, ZfsCli, Dataset, Snapshot
from zfsnappr.common.resolve_datasets import ResolvedDatasets
from zfsnappr.common.command_utils import fetch_snaps, resolve_dataset_args, resolve_filter_args, get_holds, parse_holdtags, Path, group_by, PeerInfo, ReplicationHold, get_peerinfo
from zfsnappr.common.filter import SnapFilter
from zfsnappr.common.parse_dataset_arg import ConnSpec
from zfsnappr.common.sort import sortkey_dataset
from zfsnappr.common.utils import sort_dict
from zfsnappr.common.render_table import render_table, Field


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

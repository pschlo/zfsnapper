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


if TYPE_CHECKING:
    from .args import Args


log = logging.getLogger(__name__)

Field = Field[Dataset, int]


def entrypoint(args: Args):
    """
    List all peers
    """
    resolved = resolve_dataset_args(args)

    _first = True
    for conn, (datasets, cli) in resolved.items():
        if not _first:
            log.info("")
        _first = False

        log.info(f"[{conn}] Scanning peers on {len(datasets.matched)} datasets")
        list_conn(
            conn=conn,
            datasets=datasets,
            cli=cli
        )


def list_conn(conn: ConnSpec, datasets: ResolvedDatasets, cli: ZfsCli):
    snaps = fetch_snaps(cli=cli, datasets=datasets)
    holds = get_holds(cli, snaps)

    if not snaps:
        log.info(f"No matching snapshots")
        return

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

    ds_peer_to_holds: dict[
        tuple[Path, int],
        set[tuple[Snapshot, ReplicationHold]]
    ] = {}
    for ds, _holds in _ds_to_holds.items():
        for snap, h in _holds:
            ds_peer_to_holds.setdefault((ds.path, h.guid), set()).add((snap, h))

    # Registered GUIDs PLUS those on holds
    ds_to_peers: dict[Dataset, set[int]] = {}
    for ds, _holds in _ds_to_holds.items():
        ds_to_peers[ds] = {
            *(h.guid for _, h in _holds),
            *(p.guid for p in ds.peerinfos if p)
        }

    fields = [
        Field("PATH", lambda d, p: str(d.path)),
        Field("PEER HOST", lambda d, peer: str(p.host) if (p := get_peerinfo(d, peer)) else "?"),
        Field("PEER PATH", lambda d, peer: str(p.path) if (p := get_peerinfo(d, peer)) else "?"),
        Field("HOLDS", lambda d, peer:
            str(len(ds_peer_to_holds.get((d.path, peer), [])))
        ),
        Field("LAST USED", lambda d, peer: str(p.last_used) if (p := get_peerinfo(d, peer)) else "?")
    ]
    peers = [(d, p) for d, ps in sort_dict(ds_to_peers, key=sortkey_dataset).items() for p in ps]
    render_table(fields, peers)

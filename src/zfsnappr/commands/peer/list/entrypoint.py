from __future__ import annotations
from typing import cast, Optional, TYPE_CHECKING
import logging
from collections.abc import Collection

from zfsnappr.common.zfs import ZfsProperty, ZfsCli, Dataset, Snapshot
from zfsnappr.common.resolve_datasets import ResolvedDatasets
from zfsnappr.common.command_utils import fetch_snaps, resolve_dataset_args, resolve_filter_args, get_holds, parse_holdtags, Path, group_by, PeerInfo, ReplicationHold
from zfsnappr.common.filter import SnapFilter
from zfsnappr.common.parse_dataset_arg import ConnSpec
from zfsnappr.common.sort import sortkey_dataset
from zfsnappr.common.render_table import render_table, Field


if TYPE_CHECKING:
    from .args import Args


log = logging.getLogger(__name__)


def entrypoint(args: Args):
    """
    List all peers
    """
    resolved = resolve_dataset_args(args)

    for conn, (datasets, cli) in resolved.items():
        list_conn(
            conn=conn,
            datasets=datasets,
            cli=cli
        )


def list_conn(conn: ConnSpec, datasets: ResolvedDatasets, cli: ZfsCli):
    # Collect all peers
    peers = {p for ds in datasets.matched for p in ds.peerinfos if p}
    peers = sorted(peers, key=lambda p: sortkey_dataset(p.path))
    snaps = fetch_snaps(cli=cli, datasets=datasets)
    holds = get_holds(cli, snaps)


    dsets = sorted(datasets.matched, key=sortkey_dataset)
    dset_to_snaps = group_by(snaps, key=lambda s: s.dataset, ensure_keys=[d.path for d in dsets])
    dset_to_holds = {
        dset.path: parse_holdtags(h for s in dset_to_snaps[dset.path] for h in holds[s])
        for dset in dsets
    }

    def datasets_with_peer(guid: int):
        return [
            (d, p)
            for d in dsets for p in d.peerinfos
            if p and p.guid == guid
        ]


    fields = [
        Field("PEER HOST", lambda peer: str(peer.host)),
        Field("PEER PATH", lambda peer: str(peer.path)),
        Field("USED BY DATASETS", lambda peer: str(
            "\n".join([
                str(d.path)
                for d, p in datasets_with_peer(peer.guid)
            ])
        )),
        Field("HOLDS", lambda peer: "\n".join([
            str(len([h for h in dset_to_holds[d.path] if h.guid == peer.guid]))
            for d, p in datasets_with_peer(peer.guid)
        ])),
        Field("LAST USED", lambda peer: str(peer.last_used))
    ]
    render_table(fields, peers)
    # for peer in peers:
    #     print(peer)
    #     # print(s for s in snaps if s.)

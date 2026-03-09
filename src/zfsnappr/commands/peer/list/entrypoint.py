from __future__ import annotations
from typing import cast, Optional, TYPE_CHECKING
import logging
from collections.abc import Collection

from zfsnappr.common.zfs import ZfsCli, Dataset
from zfsnappr.common.resolve_datasets import ResolvedDatasets
from zfsnappr.common.command_utils import fetch_snaps, resolve_dataset_args, get_holds, get_peerinfo
from zfsnappr.common.parse_dataset_arg import ConnSpec
from zfsnappr.common.sort import sortkey_dataset
from zfsnappr.common.utils import sort_dict
from zfsnappr.common.replication.utils import Direction, Peering
from zfsnappr.common.render_table import render_table, Field

from ..common.get_peers import get_peers


if TYPE_CHECKING:
    from .args import Args


log = logging.getLogger(__name__)

Field = Field[Dataset, Peering]


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

    ds_to_peers, ds_peer_to_holds = get_peers(snaps, holds, datasets)

    if not snaps:
        log.info(f"No matching snapshots")
        return

    fields = [
        Field("PATH", lambda d, p: str(d.path)),
        Field("", lambda d, peer: p.peering.direction.icon if (p := get_peerinfo(d, peer)) else "?"),
        Field("PEER", lambda d, peer: str(p.host) if (p := get_peerinfo(d, peer)) else "?", align="right", header_align="left"),
        Field("", lambda d, peer: str(p.path) if (p := get_peerinfo(d, peer)) else "?"),
        Field("HOLDS", lambda d, peer:
            "\n".join(
                s.shortname for s in ds_peer_to_holds[(d.path, peer)]
            )
        ),
        Field("LAST USED", lambda d, peer: str(p.last_used) if (p := get_peerinfo(d, peer)) else "?")
    ]
    peers = [(d, p) for d, ps in sort_dict(ds_to_peers, key=sortkey_dataset).items() for p in ps]
    if not peers:
        log.info("No matching peers")
        return

    render_table(
        fields,
        peers,
        column_separators=["  ", "  ", " :: ", " | ", " | "],
        header_column_separators=["  ", "  ", "    ", " | ", " | "],
        column_separator_modes=["always", "always", "both", "always", "always"],
    )

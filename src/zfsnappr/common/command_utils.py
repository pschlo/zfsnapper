import logging
from collections.abc import Collection
from dataclasses import asdict
from dateutil.relativedelta import relativedelta
from datetime import timedelta, datetime

from zfsnappr.common.filter import SnapFilter, snapfilters
from zfsnappr.common.args import CommonArgs
from zfsnappr.common.sort import sortkey_snap_by_time
from zfsnappr.common.zfs import ZfsCli, PeeringInfo, PeerField, Dataset, Snapshot
from zfsnappr.common.path import Path
from zfsnappr.common.utils import combine_dicts, group_by, space
from zfsnappr.common.resolve_datasets import ResolvedDatasets, resolve_dataset_specs
from zfsnappr.common.parse_dataset_arg import parse_dataset_arg
from zfsnappr.common.replication.utils import parse_holdtags, Peering


log = logging.getLogger(__name__)


def resolve_dataset_args(
    args: CommonArgs | None = None,
    *,
    include_exact: Collection[str] = [],
    include_recurse: Collection[str] = [],
    exclude_exact: Collection[str] = [],
    exclude_recurse: Collection[str] = [],
    strict: bool | None = None,
):
    """Shorthand function for parsing dataset args."""
    def _parse(raw_specs: Collection[str]):
        return [parse_dataset_arg(s) for s in raw_specs]

    return combine_dicts(
        *resolve_dataset_specs(
            include_exact=_parse(include_exact or (args.inc_dataset_exact if args else [])),
            include_recurse=_parse(include_recurse or (args.inc_dataset_recurse if args else [])),
            exclude_exact=_parse(exclude_exact or (args.exc_dataset_exact if args else [])),
            exclude_recurse=_parse(exclude_recurse or (args.exc_dataset_recurse if args else [])),
            strict=strict if strict is not None else (args.strict if args else False),
        )
    )


def resolve_filter_args(
    tag_groups: Collection[str] = [],
    shortnames: Collection[str] = []
) -> SnapFilter:
    filter: SnapFilter = snapfilters.Composite()
    if tag_groups:
        # Empty tag is preserved; used as token to make it possible to match snapshots without tags.
        filter &= snapfilters.Tag([g.split(',') for g in tag_groups])
    if shortnames:
        filter &= snapfilters.Shortname(shortnames)
    return filter


def fetch_snaps(
    cli: ZfsCli,
    datasets: ResolvedDatasets,
    props: Collection[str] = [],
    filter: SnapFilter = snapfilters.ALLOW_ALL
):
    """Fetch all snapshots of the given `datasets`.

    Snapshots are sorted by creation time (ascending order) and optionally filtered.
    """
    snaps = cli.get_all_snapshots(datasets.p.matched, properties=props)
    snaps = filter.apply(snaps)
    snaps = sorted(snaps, key=sortkey_snap_by_time)
    return snaps


def get_holds(
    cli: ZfsCli,
    snapshots: Collection[Snapshot]
) -> dict[Snapshot, set[str]]:
    tags = cli.get_holdtags([s.longname for s in snapshots], userrefs={s.longname: s.num_holds for s in snapshots})
    return {s: tags[s.longname] for s in snapshots}


def _set_peerinfo_slot(
    cli: ZfsCli,
    dataset: Dataset,
    peer: PeeringInfo,
    slot: int
):
    """Serializes the peer and stores it at the given slot on the dataset."""
    cli.set_property(dataset.path, f"zfsnappr:peer:{slot}", peer.serialize())
    dataset.peerinfos[slot] = peer


def _clear_peerinfo_slot(
    cli: ZfsCli,
    dataset: Dataset,
    slot: int
):
    cli.unset_property(dataset.path, f'zfsnappr:peer:{slot}')
    dataset.peerinfos[slot] = None


def update_peerinfo(
    cli: ZfsCli,
    dataset: Dataset,
    peerinfo: PeeringInfo,
):
    """Update peer if it already exists, else add under first free slot."""
    # Find peer GUID
    curr_slot = next(
        (slot for slot, p in enumerate(dataset.peerinfos) if p is not None and p.peering == peerinfo.peering),
        None
    )
    if curr_slot is not None:
        # Peer already exists in slot; overwrite
        _set_peerinfo_slot(cli=cli, dataset=dataset, peer=peerinfo, slot=curr_slot)
        return

    # Find first free slot
    slot = next((slot for slot, p in enumerate(dataset.peerinfos) if p is None), None)
    if slot is None:
        raise RuntimeError(f"Cannot set peer on dataset {dataset.path}: no free slots")
    _set_peerinfo_slot(cli=cli, dataset=dataset, peer=peerinfo, slot=slot)


def get_peerinfo(
    dataset: Dataset,
    peering: Peering
) -> PeeringInfo | None:
    return next(
        (p for slot, p in enumerate(dataset.peerinfos) if p is not None and p.peering == peering),
        None
    )


def remove_peer(
    cli: ZfsCli,
    dataset: Dataset,
    peering: Peering,
    holds: dict[Snapshot, set[str]],
    log_indent: int = 0
):
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
        _clear_peerinfo_slot(cli=cli, dataset=dataset, slot=slot)

    # Determine peer holds on that dataset
    remove_holds: dict[Peering, set[Snapshot]] = {}
    for snap, _holds in holds.items():
        if snap.dataset != dataset.path:
            continue
        for _peering in parse_holdtags(_holds):
            if _peering == peering:
                remove_holds.setdefault(_peering, set()).add(snap)

    log.debug(_s() + f"Removing {len(remove_holds)} obsolete holds")
    for i, (hold, snaps) in enumerate(remove_holds.items()):
        cli.release_hold([s.longname for s in snaps], hold.to_tag())
        for s in snaps:
            s.num_holds -= 1
            holds[s].remove(hold.to_tag())
        log.debug(_s(1) + f"{i+1}/{len(remove_holds)} removed")

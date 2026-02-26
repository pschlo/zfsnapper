import logging
from collections.abc import Collection
from dataclasses import asdict
from dateutil.relativedelta import relativedelta
from datetime import timedelta, datetime

from zfsnappr.common.filter import SnapFilter, snapfilters
from zfsnappr.common.args import CommonArgs
from zfsnappr.common.sort import sortkey_snap_by_time
from zfsnappr.common.zfs import ZfsCli, Peer, PeerField, Dataset, Snapshot
from zfsnappr.common.path import Path
from zfsnappr.common.utils import combine_dicts, group_by
from zfsnappr.common.resolve_datasets import ResolvedDatasets, resolve_dataset_specs
from zfsnappr.common.parse_dataset_arg import parse_dataset_arg
from zfsnappr.common.replication.utils import parse_holdtags, ReplicationHold


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
    tags = cli.get_holdtags([s.longname for s in snapshots], userrefs={s.longname: s.holds for s in snapshots})
    return {s: tags[s.longname] for s in snapshots}


def _set_peer_slot(
    cli: ZfsCli,
    dataset: Dataset,
    peer: Peer,
    slot: int
):
    """Serializes the peer and stores it at the given slot on the dataset."""
    field_values: dict[PeerField, str] = {
        PeerField.GUID: str(peer.guid),
        PeerField.PATH: str(peer.path),
        PeerField.HOST: str(peer.host),
        PeerField.LAST_USED: str(int(peer.last_used.timestamp()))
    }
    value = ';'.join(f'{f}={v}' for f, v in field_values.items())
    prop = f"zfsnappr:peer:{slot}"

    cli.set_property(dataset.path, prop, value)
    dataset.peer_slots[slot] = peer


def _clear_peer_slot(
    cli: ZfsCli,
    dataset: Dataset,
    slot: int
):
    cli.unset_property(dataset.path, f'zfsnappr:peer:{slot}')
    dataset.peer_slots[slot] = None


def update_peer(
    cli: ZfsCli,
    dataset: Dataset,
    peer: Peer,
):
    """Update peer if it already exists, else add under first free slot."""
    # Find peer GUID
    curr_slot = next((slot for slot, p in dataset.peer_slots.items() if p is not None and p.guid == peer.guid), None)
    if curr_slot is not None:
        # Peer already exists in slot; overwrite
        _set_peer_slot(cli=cli, dataset=dataset, peer=peer, slot=curr_slot)
        return

    # Find first free slot
    slot = next((slot for slot, p in dataset.peer_slots.items() if p is None), None)
    if slot is None:
        raise RuntimeError(f"Cannot set peer on dataset {dataset.path}: No free slots")
    _set_peer_slot(cli=cli, dataset=dataset, peer=peer, slot=slot)


def get_peer(
    dataset: Dataset,
    guid: int
) -> Peer | None:
    return next((p for slot, p in dataset.peer_slots.items() if p is not None and p.guid == guid), None)


def prune_old_peers(
    cli: ZfsCli,
    dataset: Dataset,
    snapshots: Collection[Snapshot],
    remove_older_than: relativedelta | timedelta
):
    # Collect peers that were not used within the given timedelta.
    # This means the peer was neither send to nor received from.
    remove_peers: set[tuple[int, Peer]] = set()
    threshold = datetime.now() - remove_older_than
    for slot, peer in dataset.peer_slots.items():
        if peer is None:
            continue
        if peer.last_used < threshold:
            remove_peers.add((slot, peer))

    if not remove_peers:
        print(f"No peers to prune")
        return

    # Collect all snapshot holds
    _holds = get_holds(cli, snapshots)
    peerguid_to_snaps: dict[int, dict[ReplicationHold, set[Snapshot]]] = {}
    for snap, _holds in _holds.items():
        for h in parse_holdtags(_holds):
            (
                peerguid_to_snaps
                .setdefault(h.guid, {})
                .setdefault(h, set())
                .add(snap)
            )

    # Remove peers and snapshot holds
    for slot, peer in remove_peers:
        print("Removing peer:", peer)
        _clear_peer_slot(cli=cli, dataset=dataset, slot=slot)
        # Remove relevant holds
        for hold, snaps in peerguid_to_snaps[peer.guid].items():
            cli.release_hold([s.longname for s in snaps], hold.to_tag())


def remove_peer(
    cli: ZfsCli,
    dataset: Dataset,
    peer_guid: int
):
    slot = next((slot for slot, p in dataset.peer_slots.items() if p and p.guid == peer_guid), None)
    if slot is None:
        raise KeyError()
    _clear_peer_slot(cli=cli, dataset=dataset, slot=slot)

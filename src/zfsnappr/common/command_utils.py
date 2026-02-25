import logging
from collections.abc import Collection
from dataclasses import asdict

from zfsnappr.common.filter import SnapFilter, snapfilters
from zfsnappr.common.args import CommonArgs
from zfsnappr.common.sort import sortkey_snap_by_time
from zfsnappr.common.zfs import ZfsCli, Peer, PeerField, Dataset
from zfsnappr.common.path import Path
from zfsnappr.common.utils import combine_dicts
from zfsnappr.common.resolve_datasets import ResolvedDatasets, resolve_dataset_specs
from zfsnappr.common.parse_dataset_arg import parse_dataset_arg


log = logging.getLogger(__name__)


def resolve_dataset_args(args: CommonArgs):
    """Shorthand function for parsing dataset args."""
    def _parse(raw_specs: Collection[str]):
        return [parse_dataset_arg(s) for s in raw_specs]

    return combine_dicts(
        *resolve_dataset_specs(
            include_exact=_parse(args.inc_dataset_exact),
            include_recurse=_parse(args.inc_dataset_recurse),
            exclude_exact=_parse(args.exc_dataset_exact),
            exclude_recurse=_parse(args.exc_dataset_recurse),
            strict=args.strict
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


def set_peer(
    cli: ZfsCli,
    dataset: Dataset,
    peer: Peer,
    slot: int
):
    """Serializes the peer and stores it at the given slot on the dataset."""
    field_values: dict[PeerField, str] = {
        PeerField.GUID: str(peer.guid),
        PeerField.PATH: str(peer.path),
        PeerField.HOST: peer.host,
        PeerField.LAST_USED: str(int(peer.last_used.timestamp()))
    }
    value = ';'.join(f'{f}={v}' for f, v in field_values.items())
    prop = f"zfsnappr:peer:{slot}"

    cli.set_property(dataset.path, prop, value)
    dataset.peer_slots[slot] = peer


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
        set_peer(cli=cli, dataset=dataset, peer=peer, slot=curr_slot)
        return

    # Find first free slot
    slot = next((slot for slot, peer in dataset.peer_slots.items() if peer is None), None)
    if slot is None:
        raise RuntimeError(f"Cannot set peer on dataset {dataset.path}: No free slots")
    set_peer(cli=cli, dataset=dataset, peer=peer, slot=slot)

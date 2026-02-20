from typing import Callable, Optional, Literal
from collections.abc import Collection
from dataclasses import dataclass

from .zfs import Snapshot


@dataclass
class SnapFilter:
    """
    A snapshot passes the filter if it passes every subfilter.

    By default, all subfilters are disabled and every snapshot passes the filter.
    """

    tag_groups: Collection[Collection[str]] | None = None
    """Collection of tag groups.
    A snap passes the tag subfilter if for any tag group it has all the tags in the group.
    
    If `None` (default), tag filtering is disabled.
    """

    datasets: Collection[str] | None = None
    """Collection of dataset paths.
    A snap passes the dataset subfilter if its dataset path is in the collection.

    If `None` (default), dataset filtering is disabled.
    """

    shortnames: Collection[str] | None = None
    """Collection of shortnames.
    A snap passes the shortname subfilter if its name is in the collection.

    If `None` (default), shortname filtering is disabled.
    """


def filter_snaps(snapshots: Collection[Snapshot], filter: SnapFilter) -> list[Snapshot]:
    return [s for s in snapshots if _passes_filter(s, filter)]


def _passes_filter(snap: Snapshot, filter: SnapFilter) -> bool:
    # snap is included iff it has all the tags of one of the groups in "tag"
    if filter.tag_groups is not None:
        for tag_group in filter.tag_groups:
            tag_group = set(tag_group)
            # Normal case: snap has all group tags
            if snap.tags is not None and snap.tags >= tag_group:
                break
            # Special case: snap tags are unset and group contains UNSET
            if snap.tags is None and len(tag_group) == 1 and next(iter(tag_group)) == 'UNSET':
                break
            # Special case: snap tags are empty and group contains empty tag.
            # The empty tag serves as token to select snaps without tags.
            if snap.tags == set() and len(tag_group) == 1 and next(iter(tag_group)) == '':
                break
        else:
            return False

    if filter.datasets is not None:
        if not any(snap.dataset == d for d in filter.datasets):
            return False

    if filter.shortnames is not None:
        if not any(snap.shortname == s for s in filter.shortnames):
            return False

    return True

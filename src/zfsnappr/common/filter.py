from __future__ import annotations
from collections.abc import Collection
from abc import abstractmethod, ABC

from .zfs import Snapshot
from zfsnappr.common.path import Path


class SnapFilter(ABC):
    @abstractmethod
    def allows(self, snap: Snapshot) -> bool: ...

    def apply(self, snaps: Collection[Snapshot]) -> list[Snapshot]:
        return [s for s in snaps if self.allows(s)]
    
    def __and__(self, other: SnapFilter):
        return CompositeFilter(self, other)


class _AllowAllFilter(SnapFilter):
    def allows(self, snap: Snapshot) -> bool:
        return True

ALLOW_ALL_FILTER = _AllowAllFilter()


class _BlockAllFilter(SnapFilter):
    def allows(self, snap: Snapshot) -> bool:
        return False
    
BLOCK_ALL_FILTER = _BlockAllFilter()


class CompositeFilter(SnapFilter):
    """A snapshot passes the filter if it passes every subfilter."""
    subfilters: list[SnapFilter]

    def __init__(self, *subfilters: SnapFilter) -> None:
        self.subfilters = list(subfilters)

    def allows(self, snap: Snapshot):
        return all(subfilter.allows(snap) for subfilter in self.subfilters)
    
    def __iand__(self, other: SnapFilter):
        self.subfilters.append(other)
        return self


class ShortnameFilter(SnapFilter):
    """
    Collection of shortnames.
    A snap passes the shortname subfilter if its name is in the collection.
    """
    shortnames: set[str]

    def __init__(self, shortnames: Collection[str]) -> None:
        self.shortnames = set(shortnames)

    def allows(self, snap: Snapshot) -> bool:
        return snap.shortname in self.shortnames


class DatasetFilter(SnapFilter):
    """
    Collection of dataset paths.
    A snap passes the dataset subfilter if its dataset path is in the collection.
    """
    datasets: set[Path]

    def __init__(self, datasets: Collection[Path | str]) -> None:
        self.datasets = {Path(d) for d in datasets}

    def allows(self, snap: Snapshot) -> bool:
        return snap.dataset in self.datasets


class TagFilter(SnapFilter):
    """
    Collection of tag groups.
    A snap passes the tag subfilter if for any tag group it has all the tags in the group.
    """
    tag_groups: set[frozenset[str]]

    def __init__(self, tag_groups: Collection[Collection[str]]) -> None:
        self.tag_groups = {frozenset(g) for g in tag_groups}

    def allows(self, snap: Snapshot) -> bool:
        # snap is included iff it has all the tags of one of the groups in "tag"
        for tag_group in self.tag_groups:
            # Normal case: snap has all group tags
            if snap.tags is not None and snap.tags >= tag_group:
                return True
            # Special case: snap tags are unset and group contains UNSET
            if snap.tags is None and len(tag_group) == 1 and next(iter(tag_group)) == 'UNSET':
                return True
            # Special case: snap tags are empty and group contains empty tag.
            # The empty tag serves as token to select snaps without tags.
            if snap.tags == set() and len(tag_group) == 1 and next(iter(tag_group)) == '':
                return True
        return False


class snapfilters:
    Tag = TagFilter
    Shortname = ShortnameFilter
    ALLOW_ALL = ALLOW_ALL_FILTER
    BLOCK_ALL = BLOCK_ALL_FILTER
    Dataset = DatasetFilter
    Composite = CompositeFilter

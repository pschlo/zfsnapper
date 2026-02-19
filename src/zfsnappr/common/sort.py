from collections.abc import Collection

from zfsnappr.common.zfs import Snapshot
from .resolve_paths import path_depth


def sort_snaps_by_time(snaps: Collection[Snapshot], reverse: bool = False) -> list[Snapshot]:
    return list(sorted(
        snaps,
        key=lambda s: (s.timestamp, path_depth(s.dataset), s.dataset, s.guid),
        reverse=reverse
    ))

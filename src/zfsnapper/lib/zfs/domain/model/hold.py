from __future__ import annotations
from dataclasses import dataclass

from zfsnapper.lib.zfs.domain.model.path import Path


@dataclass(frozen=True, eq=False)
class Hold:
    dataset: Path
    snap_shortname: str
    tag: str

    @property
    def snap_longname(self):
        return f"{self.dataset}@{self.snap_shortname}"

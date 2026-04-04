from collections.abc import Collection

from . import model
from zfsnapper.common.path import Path


Snap = str | model.Snapshot
Snaps = Collection[str] | Collection[model.Snapshot]

Dataset = str | Path | model.Dataset
Datasets = Collection[str] | Collection[Path] | Collection[model.Dataset]

Pool = str | model.Pool
Pools = Collection[str] | Collection[model.Pool]

AnySingle = Snap | Dataset | Pool
AnyCollection = Snaps | Datasets | Pools

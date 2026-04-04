from collections.abc import Collection

from . import model


Snap = str | model.Snapshot
Snaps = Collection[str] | Collection[model.Snapshot]

Dataset = str | model.Path | model.Dataset
Datasets = Collection[str] | Collection[model.Path] | Collection[model.Dataset]

Pool = str | model.Pool
Pools = Collection[str] | Collection[model.Pool]

AnySingle = Snap | Dataset | Pool
AnyCollection = Snaps | Datasets | Pools

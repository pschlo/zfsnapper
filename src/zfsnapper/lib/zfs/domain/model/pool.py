from __future__ import annotations
from dataclasses import dataclass
from collections.abc import Collection

from .enums import PropertyName
from zfsnapper.lib.zfs import Property


@dataclass(frozen=True, eq=False)
class Pool:
    name: str
    guid: int

    @classmethod
    def from_props(cls, properties: Collection[Property]):
        P = PropertyName
        ps = {p.propname: p for p in properties}

        name = ps[P.NAME].value
        guid = int(ps[P.GUID].value)

        return Pool(
            name=name,
            guid=guid,
        )

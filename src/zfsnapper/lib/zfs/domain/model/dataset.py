from __future__ import annotations
from dataclasses import dataclass
from collections.abc import Collection

from ...raw import Property, ZfsDatasetType, PropertySource
from zfsnapper.lib.zfs import Path
from .peering import PeeringInfo, Peering
from .enums import PropertyName


@dataclass(frozen=True, eq=False)
class Dataset:
    path: Path
    guid: int
    type: ZfsDatasetType
    is_encrypted: bool
    peerinfos: list[PeeringInfo | None]

    def __repr__(self) -> str:
        return f"Dataset({self.path})"
    
    @property
    def poolname(self) -> str:
        return self.path[0]

    @classmethod
    def from_props(cls, properties: Collection[Property]):
        P = PropertyName
        ps = {p.propname: p for p in properties}

        path = Path(ps[P.NAME].value)
        guid = int(ps[P.GUID].value)
        type = ZfsDatasetType(ps[P.TYPE].value)
        is_encrypted = ps[P.ENCRYPTION].value != 'off'

        # Parse peer slots
        peer_slots_dict: dict[int, PeeringInfo | None] = {}
        for propkey, prop in ps.items():
            parts = propkey.split(':')
            if parts[:2] != ['zfsnapper', 'peer']:
                continue

            slot = int(parts[2])
            # Ignore inherited peer slots
            if prop.value == '-' or prop.source != PropertySource.LOCAL:
                # Slot is empty
                peer_slots_dict[slot] = None
                continue

            # Slot is nonempty
            fields = {}
            for field in prop.value.split(';'):
                f, v = field.split('=', maxsplit=1)
                fields[f] = v
            peer_slots_dict[slot] = PeeringInfo.from_fields(fields)

        # Convert peer slots to list.
        # Raises KeyError if slots are not contiguous.
        if not peer_slots_dict:
            peerinfo_slots = []
        else:
            max_slot = max(peer_slots_dict.keys())
            peerinfo_slots = [peer_slots_dict[i] for i in range(max_slot + 1)]

        # Assert no peering is duplicated, i.e. at most 1 info per peering
        _seen: set[Peering] = set()
        for p in peerinfo_slots:
            if p is None:
                continue
            if p.peering in _seen:
                raise ValueError(f"Duplicate peering: {p.peering}")
            _seen.add(p.peering)

        return Dataset(
            path=path,
            guid=guid,
            type=type,
            is_encrypted=is_encrypted,
            peerinfos=peerinfo_slots
        )
    
    def get_peerinfo(self, peering: Peering) -> PeeringInfo | None:
        return next(
            (p for slot, p in enumerate(self.peerinfos) if p is not None and p.peering == peering),
            None
        )
    
    def with_peerinfo_slot(self, slot: int, info: PeeringInfo | None) -> Dataset:
        peerinfos = self.peerinfos
        peerinfos[slot] = info

        return Dataset(
            path=self.path,
            guid=self.guid,
            type=self.type,
            is_encrypted=self.is_encrypted,
            peerinfos=peerinfos
        )

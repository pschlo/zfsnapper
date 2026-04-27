from dataclasses import dataclass
from datetime import datetime
from collections.abc import Collection
from abc import ABC, abstractmethod

from .runner import CommandRunner
from .model import ZfsModel


class BackendCommand[T](ABC):
    """
    one concrete ZFS call + model projection

    - Results in exactly one ZFS command execution
    - Updates the model accordingly
    """
    @abstractmethod
    def execute(self, runner: CommandRunner) -> T: ...

    @abstractmethod
    def project(self, result: T, model: ZfsModel) -> None:
        """Mutate the model in-place."""
        ...


@dataclass(frozen=True, slots=True)
class UniqueSnapKey:
    dataset_guid: int
    snapshot_guid: int


@dataclass(frozen=True, slots=True)
class LimitedSnapInfo:
    dataset: Path
    shortname: str
    guid: int
    timestamp: datetime
    tags: frozenset[str] | None
    num_holds: int

    properties: dict[str, Property]
    """Properties as fetched from ZFS; may be outdated."""

    def __repr__(self) -> str:
        return f"Snapshot({self.longname})"

    @classmethod
    def from_props(cls, properties: Collection[Property]):
        P = PropertyName
        ps = {p.propname: p for p in properties}

        dataset_name, shortname = ps[P.NAME].value.split('@')
        dataset = Path(dataset_name)
        guid = int(ps[P.GUID].value)
        timestamp = datetime.fromtimestamp(int(ps[P.CREATION].value))
        num_holds = int(ps[P.USERREFS].value)

        if ps[P.ZFSNAPPER_TAGS].value == '-':
            tags = None
        else:
            tags = frozenset(t for t in ps[P.ZFSNAPPER_TAGS].value.split(',') if t)  # ignore empty tags

        return cls(
            dataset=dataset,
            shortname=shortname,
            guid=guid,
            timestamp=timestamp,
            tags=tags,
            num_holds=num_holds,
            properties=ps
        )




@dataclass(frozen=True, slots=True)
class LimitedDatasetInfo:
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

        return LimitedDatasetInfo(
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

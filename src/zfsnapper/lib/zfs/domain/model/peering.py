from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime

from zfsnapper.lib.cli.parse_dataset_arg import ConnSpec
from zfsnapper.lib.zfs import Path
from .enums import PeerField, Direction


@dataclass(frozen=True, eq=False)
class PeeringInfo:
    peering: Peering
    host: ConnSpec
    path: Path
    pool_guid: int
    last_used: datetime

    @classmethod
    def from_fields(cls, fields: dict[str, str]):
        P = PeerField
        fs = fields
        return PeeringInfo(
            peering=Peering(
                direction=Direction(fs[P.DIRECTION]),
                guid=int(fs[P.GUID])
            ),
            host=ConnSpec.parse(fs[P.HOST]),
            path=Path(fs[P.PATH]),
            pool_guid=int(fs[P.POOL_GUID]),
            last_used=datetime.fromtimestamp(int(fs[P.LAST_USED]))
        )

    def serialize(self, localhost: str | None = None) -> str:
        field_values: dict[PeerField, str] = {
            PeerField.DIRECTION: str(self.peering.direction),
            PeerField.GUID: str(self.peering.guid),
            PeerField.PATH: str(self.path),
            PeerField.HOST: self.host.serialize(localhost=localhost),
            PeerField.POOL_GUID: str(self.pool_guid),
            PeerField.LAST_USED: str(int(self.last_used.timestamp()))
        }
        return ';'.join(f'{f}={v}' for f, v in field_values.items())


@dataclass(frozen=True, eq=True)
class Peering:
    direction: Direction
    guid: int

    @classmethod
    def from_tag(cls, tag: str):
        if tag.startswith('zfsnapper-recvbase-'):
            return Peering(
                Direction.RECEIVE,
                int(tag.removeprefix('zfsnapper-recvbase-'))
            )
        if tag.startswith('zfsnapper-sendbase-'):
            return Peering(
                Direction.SEND,
                int(tag.removeprefix('zfsnapper-sendbase-'))
            )
        raise ValueError(f"Invalid holdtag")
    
    def to_tag(self) -> str:
        match self.direction:
            case Direction.SEND:
                return f"zfsnapper-sendbase-{self.guid}"
            case Direction.RECEIVE:
                return f"zfsnapper-recvbase-{self.guid}"
            case _:
                assert False

from collections.abc import Collection, Iterable
from dataclasses import dataclass
from enum import StrEnum


class Direction(StrEnum):
    SEND = 'send'
    RECEIVE = 'receive'

    @property
    def icon(self):
        return '🡒' if self == Direction.SEND else '🡐'


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


def parse_holdtags(tags: Iterable[str]) -> list[Peering]:
    res: list[Peering] = []
    for tag in tags:
        try:
            res.append(Peering.from_tag(tag))
        except ValueError:
            pass
    return res

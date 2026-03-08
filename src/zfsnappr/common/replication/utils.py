from collections.abc import Collection, Iterable
from dataclasses import dataclass
from enum import StrEnum


class Direction(StrEnum):
    SEND = 'send'
    RECEIVE = 'receive'


@dataclass(frozen=True, eq=True)
class Peering:
    direction: Direction
    guid: int

    @classmethod
    def from_tag(cls, tag: str):
        if tag.startswith('zfsnappr-recvbase-'):
            return Peering(
                Direction.RECEIVE,
                int(tag.removeprefix('zfsnappr-recvbase-'))
            )
        if tag.startswith('zfsnappr-sendbase-'):
            return Peering(
                Direction.SEND,
                int(tag.removeprefix('zfsnappr-sendbase-'))
            )
        raise ValueError(f"Invalid holdtag")
    
    def to_tag(self) -> str:
        match self.direction:
            case Direction.SEND:
                return f"zfsnappr-sendbase-{self.guid}"
            case Direction.RECEIVE:
                return f"zfsnappr-recvbase-{self.guid}"
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

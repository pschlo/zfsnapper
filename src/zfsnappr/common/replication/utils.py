from collections.abc import Collection
from dataclasses import dataclass
from enum import StrEnum

from zfsnappr.common.zfs import Dataset


class Direction(StrEnum):
    SEND = 'send'
    RECEIVE = 'receive'


@dataclass(frozen=True, eq=True)
class ReplicationHold:
    direction: Direction
    guid: int

    @classmethod
    def from_tag(cls, tag: str):
        if tag.startswith('zfsnappr-recvbase-'):
            return ReplicationHold(
                Direction.RECEIVE,
                int(tag.removeprefix('zfsnappr-recvbase-'))
            )
        if tag.startswith('zfsnappr-sendbase-'):
            return ReplicationHold(
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


def parse_holdtags(tags: Collection[str]) -> list[ReplicationHold]:
    res: list[ReplicationHold] = []
    for tag in tags:
        try:
            res.append(ReplicationHold.from_tag(tag))
        except ValueError:
            pass
    return res

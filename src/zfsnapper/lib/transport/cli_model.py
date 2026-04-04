from dataclasses import dataclass
from enum import StrEnum


class PropertySource(StrEnum):
    NONE = "none"
    INHERITED = "inherited"
    DEFAULT = "default"
    LOCAL = "local"

class ZfsDatasetType(StrEnum):
    FILESYSTEM = 'filesystem'
    VOLUME = 'volume'
    SNAPSHOT = 'snapshot'
    BOOKMARK = 'bookmark'


@dataclass(frozen=True)
class Property:
    objname: str
    propname: str
    value: str
    source: PropertySource

    @classmethod
    def from_raw(cls, name: str, property: str, value: str, source: str):
        return Property(
            objname=name,
            propname=property,
            value=value,
            source=parse_property_source(source)
        )


def parse_property_source(source: str) -> PropertySource:
    if source == "-":
        return PropertySource.NONE
    if source == "local":
        return PropertySource.LOCAL
    if source.startswith("inherited"):
        return PropertySource.INHERITED
    if source == "default":
        return PropertySource.DEFAULT
    raise ValueError(f"Invalid property source")


@dataclass(eq=True, frozen=True)
class Hold:
    snap_longname: str
    tag: str

    @classmethod
    def from_raw(cls, name: str, tag: str):
        return Hold(
            snap_longname=name,
            tag=tag
        )

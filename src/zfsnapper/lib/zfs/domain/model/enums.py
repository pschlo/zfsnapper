from __future__ import annotations
from enum import StrEnum


class Direction(StrEnum):
    SEND = 'send'
    RECEIVE = 'receive'

    @property
    def icon(self):
        return '🡒' if self == Direction.SEND else '🡐'


class PropertyName(StrEnum):
    NAME = 'name'
    CREATION = 'creation'
    GUID = 'guid'
    USERREFS = 'userrefs'
    READONLY = 'readonly'
    ATIME = 'atime'
    MOUNTPOINT = 'mountpoint'
    CANMOUNT = 'canmount'
    TYPE = 'type'
    ENCRYPTION = 'encryption'
    ZFSNAPPER_TAGS = 'zfsnapper:tags'  # the user property used to store and read tags
    SHARENFS = 'sharenfs'
    SHARESMB = 'sharesmb'
    QUOTA = 'quota'
    RESERVATION = 'reservation'
    REFRESERVATION = 'refreservation'
    COMPRESSION = 'compression'
    RECORDSIZE = 'recordsize'


class PeerField(StrEnum):
    """Used for custom user properties of the format `zfsnapper:peer:<slot>:<property>`."""
    DIRECTION = 'direction'
    GUID = 'guid'
    HOST = 'host'
    PATH = 'path'
    POOL_GUID = 'pool_guid'
    LAST_USED = 'last_used'


PEER_SLOT_PROPERTIES = [f'zfsnapper:peer:{i}' for i in range(50)]

# properties that will always be fetched
REQUIRED_SNAP_PROPS = [
    PropertyName.NAME,
    PropertyName.CREATION,
    PropertyName.GUID,
    PropertyName.ZFSNAPPER_TAGS,
    PropertyName.USERREFS,
]

REQUIRED_DATASET_PROPS = [
    PropertyName.NAME,
    PropertyName.GUID,
    PropertyName.TYPE,
    PropertyName.ENCRYPTION
] + PEER_SLOT_PROPERTIES

REQUIRED_POOL_PROPS = [
    PropertyName.NAME,
    PropertyName.GUID
]


ALL_ZFS_PROPERTIES: list[str] = list(PropertyName) + PEER_SLOT_PROPERTIES

UNEXCLUDABLE_RECEIVE_PROPS = [
    PropertyName.NAME,
    PropertyName.TYPE,
    PropertyName.CREATION,
    PropertyName.USERREFS,
    PropertyName.GUID,
    PropertyName.ENCRYPTION
]
"""Properties which cannot be passed for `zfs receive -x`"""

EXCLUDABLE_RECEIVE_PROPS = set(ALL_ZFS_PROPERTIES) - set(UNEXCLUDABLE_RECEIVE_PROPS)

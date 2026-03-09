from dataclasses import dataclass
import string
import platform
from .path import Path


LOCAL_NODE_TOKEN = "local"
LOCAL_NODE_NAME = platform.node()


@dataclass(frozen=True, eq=True)
class ConnSpec:
    host: str | None
    user: str | None
    port: int | None

    def __str__(self) -> str:
        return self.serialize()

    def serialize(self, localhost: str | None = None) -> str:
        if self.host is None:
            return localhost or LOCAL_NODE_NAME

        res = self.host
        if self.user:
            res = f"{self.user}@{res}"
        if self.port:
            res = f"{res}:{self.port}"
        return res

    @classmethod
    def parse(cls, value: str):
        # Split value into user + hostport
        if value:
            _parts = value.split('@')
            if not all(_parts):
                raise ConnParseError(value)
            if len(_parts) == 1:
                user, _hostport = None, _parts[0]
            elif len(_parts) == 2:
                user, _hostport = _parts
            else:
                raise ConnParseError(value)
        else:
            user, _hostport = None, None

        # Split hostport into host + port
        if _hostport is not None:
            _parts = _hostport.rsplit(':', maxsplit=1)
            if not all(_parts):
                raise ConnParseError(value)
            if len(_parts) == 1:
                host, port = _parts[0], None
            elif len(_parts) == 2:
                host, port = _parts[0], int(_parts[1])
            else:
                raise ConnParseError(value)
        else:
            host, port = None, None

        if host == LOCAL_NODE_TOKEN and port is None and user is None:
            # Equivalent to omitted host, i.e. local system
            host = None

        # Validate
        if not all([
            not user or is_alnum(user),
            not host or is_alnum(host),
            host or (not user and not port)  # not host IMPLIES not user and not port
        ]):
            raise ConnParseError(value)

        return cls(
            user=user,
            host=host,
            port=port
        )



@dataclass(frozen=True, eq=True)
class DatasetSpec:
    conn: ConnSpec
    dataset: Path


class DatasetParseError(Exception):
    def __init__(self, spec: str) -> None:
        super().__init__(f"Invalid dataset spec '{spec}'")

class ConnParseError(Exception):
    def __init__(self, spec: str) -> None:
        super().__init__(f"Invalid connection spec '{spec}'")


ALNUM = set(string.ascii_letters + string.digits + '_-')

def is_alnum(value: str):
    return value and set(value) <= ALNUM


def parse_dataset_arg(arg: str) -> DatasetSpec:
    """Returned dataset path may be empty path."""
    dataset: str

    # value = conn::dataset
    # conn = user@hostport
    # hostport = host:port
    # --> value_resolved = user@host:port::dataset

    # Split dataset path from domain/netloc.
    # Dataset may be empty string.
    _parts = arg.rsplit('::', maxsplit=1)
    if len(_parts) == 1:
        _conn, dataset = _parts[0], ""
    elif len(_parts) == 2:
        _conn, dataset = _parts[0], _parts[1]
    else:
        assert False

    if not (
        not dataset or all(map(is_alnum, dataset.split('/')))
    ):
        raise DatasetParseError(arg)

    conn = ConnSpec.parse(_conn)
    return DatasetSpec(
        conn=conn,
        dataset=Path(dataset)
    )

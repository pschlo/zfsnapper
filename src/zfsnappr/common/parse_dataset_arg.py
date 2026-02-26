from dataclasses import dataclass
import string
from .path import Path


@dataclass(frozen=True, eq=True)
class ConnSpec:
    host: str | None
    user: str | None
    port: int | None

    def __str__(self) -> str:
        if self.host is None:
            return "LOCAL"

        res = self.host
        if self.user:
            res = f"{self.user}@{res}"
        if self.port:
            res = f"{res}:{self.port}"
        return res


@dataclass(frozen=True)
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

    # value = conn/dataset
    # conn = user@hostport
    # hostport = host:port
    # --> value_resolved = user@host:port/dataset

    # Split dataset path from domain/netloc.
    # Dataset may be empty string.
    _parts = arg.split('/', maxsplit=1)
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

    conn = parse_conn(_conn)
    return DatasetSpec(
        conn=conn,
        dataset=Path(dataset)
    )


def parse_conn(value: str) -> ConnSpec:
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

    # Validate
    if not all([
        not user or is_alnum(user),
        not host or is_alnum(host),
    ]):
        raise ConnParseError(value)
    
    return ConnSpec(
        user=user,
        host=host,
        port=port
    )

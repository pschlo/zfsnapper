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
      return "local"

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


ALNUM = set(string.ascii_letters + string.digits + '_-')

def is_alnum(value: str):
  return value and set(value) <= ALNUM


def parse_dataset_spec(raw_spec: str):
  """Dataset path may be empty path."""
  user: str | None
  host: str | None
  port: int | None
  dataset: str

  # value = netloc/dataset
  # netloc = user@hostport
  # hostport = host:port
  # value_resolved = user@host:port/dataset

  # Split dataset path from domain/netloc.
  # Dataset may be empty string.
  _parts = raw_spec.split('/', maxsplit=1)
  if len(_parts) == 1:
    _netloc, dataset = _parts[0] or None, ""
  elif len(_parts) == 2:
    _netloc, dataset = _parts[0] or None, _parts[1]
  else:
    assert False

  if _netloc is not None:
    _parts = _netloc.split('@')
    if not all(_parts):
      raise DatasetParseError(raw_spec)
    if len(_parts) == 1:
      user, _hostport = None, _parts[0]
    elif len(_parts) == 2:
      user, _hostport = _parts
    else:
      raise DatasetParseError(raw_spec)
  else:
    user, _hostport = None, None

  if _hostport is not None:
    _parts = _hostport.rsplit(':', maxsplit=1)
    if not all(_parts):
      raise DatasetParseError(raw_spec)
    if len(_parts) == 1:
      host, port = _parts[0], None
    elif len(_parts) == 2:
      host, port = _parts[0], int(_parts[1])
    else:
      raise DatasetParseError(raw_spec)
  else:
    host, port = None, None

  # Validate
  if not all([
    not user or is_alnum(user),
    not host or is_alnum(host),
    not dataset or all(map(is_alnum, dataset.split('/')))
  ]):
    raise DatasetParseError(raw_spec)
  
  conn = ConnSpec(
    user=user,
    host=host,
    port=port
  )
  return DatasetSpec(
    conn=conn,
    dataset=Path(dataset)
  )

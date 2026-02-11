from typing import Callable, Optional, Literal
from dataclasses import dataclass
from collections import defaultdict
from collections.abc import Collection, Hashable, Iterable
import string

from .zfs import Snapshot, LocalZfsCli, RemoteZfsCli, ZfsCli


def group_by[Group: Hashable, Item](iterable: Iterable[Item], key: Callable[[Item], Group]) -> dict[Group, list[Item]]:
  groups: dict[Group, list[Item]] = {}
  for item in iterable:
    g = key(item)
    if g not in groups:
      groups[g] = []
    groups[g].append(item)
  return groups


class DatasetParseError(Exception):
  def __init__(self, spec: str) -> None:
    super().__init__(f"Invalid dataset spec '{spec}'")


ALNUM = set(string.ascii_letters + string.digits + '_-')

def is_alnum(value: str):
  return value and set(value) <= ALNUM


@dataclass(frozen=True)
class DatasetSpec:
  user: str | None
  host: str | None
  port: int | None
  dataset: str | None


def read_dataset_spec(raw_spec: str):
  user: str | None
  host: str | None
  port: int | None
  dataset: str | None

  # value = netloc/dataset
  # netloc = user@hostport
  # hostport = host:port
  # value_resolved = user@host:port/dataset

  # split dataset path from domain/netloc
  _parts = raw_spec.split('/', maxsplit=1)
  if len(_parts) == 1:
    _netloc, dataset = _parts[0], None
  elif len(_parts) == 2:
    _netloc, dataset = _parts[0] or None, _parts[1] or None
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

  return DatasetSpec(
    user=user,
    host=host,
    port=port,
    dataset=dataset
  )


def get_zfs_cli(value: str | None) -> tuple[ZfsCli, str | None]:
  if value is None:
    return LocalZfsCli(), None

  config = read_dataset_spec(value)
  if config.host:
    cli = RemoteZfsCli(
      host=config.host,
      user=config.user,
      port=config.port
    )
  else:
    cli = LocalZfsCli()

  return cli, config.dataset


@dataclass(frozen=True, eq=True)
class ConnectionSpec:
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


def _create_zfs_cli(host: str | None, user: str | None, port: int | None) -> ZfsCli:
  if host:
    return RemoteZfsCli(
      host=host,
      user=user,
      port=port
    )
  else:
    return LocalZfsCli()


def parse_datasets(raw_specs: list[str]) -> dict[ConnectionSpec, list[str | None]]:
  specs = [read_dataset_spec(spec) for spec in raw_specs]
  groups = group_by(specs, key=lambda s: ConnectionSpec(host=s.host, user=s.user, port=s.port))
  return {conn: [s.dataset for s in _specs] for conn, _specs in groups.items()}


def create_zfs_clis(conns: Collection[ConnectionSpec]) -> dict[ConnectionSpec, ZfsCli]:
  return {
    c: _create_zfs_cli(host=c.host, user=c.user, port=c.port)
    for c in conns
  }

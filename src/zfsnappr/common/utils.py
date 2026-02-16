from typing import Callable, Optional, Literal, cast
from dataclasses import dataclass
from collections import defaultdict
from collections.abc import Collection, Hashable, Iterable
import string

from .zfs import Snapshot, LocalZfsCli, RemoteZfsCli, ZfsCli, Dataset


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


def parse_datasets(raw_specs: Collection[str]) -> dict[ConnectionSpec, list[str | None]]:
  specs = [read_dataset_spec(spec) for spec in raw_specs]
  groups = group_by(specs, key=lambda s: ConnectionSpec(host=s.host, user=s.user, port=s.port))
  return {conn: [s.dataset for s in _specs] for conn, _specs in groups.items()}


def create_zfs_clis(conns: Collection[ConnectionSpec]) -> dict[ConnectionSpec, ZfsCli]:
  return {
    c: _create_zfs_cli(host=c.host, user=c.user, port=c.port)
    for c in conns
  }




def fullparse_datasets(
  specs: Collection[str],
  exclude_specs: Collection[str],
  recursive: bool
) -> tuple[
  dict[ConnectionSpec, list[Dataset]],
  dict[ConnectionSpec, ZfsCli]
]:
  raw_datasets = parse_datasets(specs)
  exclude_datasets = parse_datasets(exclude_specs)
  clis = create_zfs_clis(list(raw_datasets))

  datasets: dict[ConnectionSpec, list[Dataset]] = {}
  for conn, _datasets in raw_datasets.items():
    # Determine which datasets to fetch
    _fetch_datasets: list[str] | None
    if None in _datasets:
      if recursive:
        # Fetch everything
        _fetch_datasets = None
      else:
        raise ValueError(f"Cannot act on empty dataset path directly, must use recursion")
    else:
      _fetch_datasets = cast(list[str], _datasets)

    # Fetch datasets
    ds: list[Dataset] = clis[conn].get_all_datasets(_fetch_datasets, recursive=recursive)

    # Remove datasets that are excluded
    filtered_ds: list[Dataset] = []
    _exclude_ds = exclude_datasets.get(conn, [])
    if None in _exclude_ds and not recursive:
      raise ValueError(f"Cannot exclude empty dataset path directly, must use recursion")
    for d in ds:
      if recursive:
        # Recursive; check if prefix is excluded
        # The empty dataset (None) is a prefix of everything
        if any(x is None or d.name.startswith(x) for x in _exclude_ds):
          continue
      else:
        # Non-recursive; check if name is directly excluded
        if any(d.name == x for x in _exclude_ds):
          continue
      filtered_ds.append(d)

    datasets[conn] = filtered_ds

  return datasets, clis







def parts(path: str) -> list[str]:
    return [x for x in path.split("/") if x]

def all_prefixes(path: str):
    cur = []
    for x in parts(path):
        cur.append(x)
        yield "/".join(cur)

def parent(path: str):
    return path.rsplit("/", 1)[0] if "/" in path else None

def compute_blocked_by_ancestor(prefixes: set[str], excluded: set[str]) -> dict[str, bool]:
    blocked: dict[str, bool] = {}
    for p in sorted(prefixes, key=lambda s: s.count("/")):  # parent before child
        par = parent(p)
        blocked[p] = (par in excluded) or (par is not None and blocked.get(par, False))
    return blocked

def counting_groups(included: Collection[str], excluded: Collection[str], exclude_blocks_descendants: bool) -> list[str]:
    excluded = set(excluded)
    has_inc: set[str] = set()
    has_exc: set[str] = set()

    # Count for each prefix path how many includes and excludes it matches.
    # This tells us for each prefix if there are includes and/or excludes under it
    for path in included:
        for prefix in all_prefixes(path):
            has_inc.add(prefix)
    for path in excluded:
        for prefix in all_prefixes(path):
            has_exc.add(prefix)

    is_blocked = compute_blocked_by_ancestor(has_inc, excluded)
    def is_safe(prefix: str) -> bool:
        """A prefix is safe iff no excluded datasets are at/under it"""
        if prefix in has_exc:
            return False  # excluded at or under prefix
        if exclude_blocks_descendants and is_blocked[prefix]:
            return False  # excluded above prefix blocks descendants
        return True

    groups: list[str] = []
    for prefix in has_inc:                      # only prefixes that occur on included paths
        if not is_safe(prefix):
            continue
        par = parent(prefix)
        if par is None or not is_safe(par):    # parent unsafe => this is a maximal safe prefix
            groups.append(prefix)

    groups.sort(key=lambda s: (s.count("/"), s))
    return groups

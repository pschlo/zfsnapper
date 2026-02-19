from typing import Callable, Optional, Literal, cast
from dataclasses import dataclass
from collections.abc import Collection

from .zfs import ZfsCli, Dataset, RemoteZfsCli, LocalZfsCli
from .resolve_paths import resolve_paths
from .parse_dataset_spec import parse_dataset_spec
from .utils import group_by


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


@dataclass
class Policy:
   include_exact: set[str | None]
   include_recurse: set[str | None]
   exclude_exact: set[str | None]
   exclude_recurse: set[str | None]


@dataclass
class ResolvedDatasets:
   kept_datasets: set[Dataset]
   single_datasets: set[Dataset]
   recursive_groups: set[Dataset]


def create_zfs_cli(conn: ConnectionSpec) -> ZfsCli:
  if conn.host:
    return RemoteZfsCli(
      host=conn.host,
      user=conn.user,
      port=conn.port
    )
  else:
    return LocalZfsCli()


def parse_dataset_specs(raw_specs: Collection[str]) -> dict[ConnectionSpec, list[str | None]]:
  specs = [parse_dataset_spec(spec) for spec in raw_specs]
  groups = group_by(specs, key=lambda s: ConnectionSpec(host=s.host, user=s.user, port=s.port))
  return {conn: [s.dataset for s in _specs] for conn, _specs in groups.items()}


def resolve_datasets(
    include_exact: Collection[str],
    include_recurse: Collection[str],
    exclude_exact: Collection[str],
    exclude_recurse: Collection[str]
) -> tuple[
    dict[ConnectionSpec, ResolvedDatasets],
    dict[ConnectionSpec, ZfsCli]
]:
    _include_exact_parsed = parse_dataset_specs(include_exact)
    _include_recurse_parsed = parse_dataset_specs(include_recurse)
    _exclude_exact_parsed = parse_dataset_specs(exclude_exact)
    _exclude_recurse_parsed = parse_dataset_specs(exclude_recurse)

    conns = _include_exact_parsed.keys() | _include_recurse_parsed.keys()
    clis = {c: create_zfs_cli(c) for c in conns}
    policies = {
       conn: Policy(
          include_exact=set(_include_exact_parsed.get(conn, [])),
          include_recurse=set(_include_recurse_parsed.get(conn, [])),
          exclude_exact=set(_exclude_exact_parsed.get(conn, [])),
          exclude_recurse=set(_exclude_recurse_parsed.get(conn, []))
       )
       for conn in conns
    }

    datasets: dict[ConnectionSpec, ResolvedDatasets] = {}
    for conn, policy in policies.items():
        all_datasets: list[Dataset] = clis[conn].get_all_datasets()
        path_to_dataset: dict[str, Dataset] = {d.name: d for d in all_datasets}

        # Resolve dataset paths
        resolved_paths = resolve_paths(
           all_datasets=[d.name for d in all_datasets],
           included_exact=[d or "" for d in policy.include_exact],
           included_recurse=[d or "" for d in policy.include_recurse],
           excluded_exact=[d or "" for d in policy.exclude_exact],
           excluded_recurse=[d or "" for d in policy.exclude_recurse],
        )

        # Ensure there are kept datasets
        if not resolved_paths.kept_datasets:
           raise ValueError(f"Resolving datasets for location '{conn}' yielded no datasets")

        # Ensure all explicitly included datasets are kept, to avoid surprises
        for d in policy.include_exact:
           if d and d not in resolved_paths.kept_datasets:
              raise ValueError(f"Dataset '{conn}/{d}' is no longer included in resolved datasets")

        # Reconstruct datasets
        resolved_datasets = ResolvedDatasets(
           kept_datasets={path_to_dataset[d] for d in resolved_paths.kept_datasets},
           single_datasets={path_to_dataset[d] for d in resolved_paths.single_datasets},
           # In ZFS, parents must exist, so this is safe
           recursive_groups={path_to_dataset[d] for d in resolved_paths.recursive_groups}
        )

        datasets[conn] = resolved_datasets

    return datasets, clis

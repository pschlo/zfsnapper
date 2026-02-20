from dataclasses import dataclass
from collections.abc import Collection

from .zfs import ZfsCli, Dataset, RemoteZfsCli, LocalZfsCli
from .resolve_paths import resolve_paths
from .path import Path
from .parse_dataset_spec import parse_dataset_spec, ConnSpec
from .utils import group_by
from .sort import sort_conns


@dataclass
class Policy:
    include_exact: set[Path]
    include_recurse: set[Path]
    exclude_exact: set[Path]
    exclude_recurse: set[Path]


@dataclass
class ResolvedDatasets:
    kept_datasets: set[Dataset]
    single_datasets: set[Dataset]
    recursive_groups: set[Dataset]


def create_zfs_cli(conn: ConnSpec) -> ZfsCli:
    if conn.host:
        return RemoteZfsCli(
            host=conn.host,
            user=conn.user,
            port=conn.port
        )
    else:
        return LocalZfsCli()


def parse_dataset_specs(raw_specs: Collection[str]) -> dict[ConnSpec, list[Path]]:
    """Dataset path may be empty path."""
    specs = [parse_dataset_spec(spec) for spec in raw_specs]
    groups = group_by(specs, key=lambda s: s.conn)
    return {conn: [s.dataset for s in _specs] for conn, _specs in groups.items()}


def resolve_datasets(
    include_exact: Collection[str],
    include_recurse: Collection[str],
    exclude_exact: Collection[str],
    exclude_recurse: Collection[str],
    strict: bool = False
) -> tuple[
    dict[ConnSpec, ResolvedDatasets],
    dict[ConnSpec, ZfsCli]
]:
    _include_exact_parsed = parse_dataset_specs(include_exact)
    _include_recurse_parsed = parse_dataset_specs(include_recurse)
    _exclude_exact_parsed = parse_dataset_specs(exclude_exact)
    _exclude_recurse_parsed = parse_dataset_specs(exclude_recurse)

    # Collect all appearing connections.
    inc_conns = _include_exact_parsed.keys() | _include_recurse_parsed.keys()
    exc_conns = _exclude_exact_parsed.keys() | _exclude_recurse_parsed.keys()
    if not inc_conns:
        raise ValueError(f"No dataset locations specified")
    if diff := exc_conns - inc_conns:
        raise ValueError(f"Location '{next(iter(diff))}' is only used for exclusion")
    
    # Sort conns for determinism
    conns = sort_conns(inc_conns)

    # Create CLIs
    clis = {c: create_zfs_cli(c) for c in conns}

    # For each conn, determine include/exclude policy.
    policies = {
        conn: Policy(
            include_exact=set(_include_exact_parsed.get(conn, [])),
            include_recurse=set(_include_recurse_parsed.get(conn, [])),
            exclude_exact=set(_exclude_exact_parsed.get(conn, [])),
            exclude_recurse=set(_exclude_recurse_parsed.get(conn, []))
        )
        for conn in conns
    }

    # For each conn, apply its policy.
    datasets: dict[ConnSpec, ResolvedDatasets] = {}
    for conn, policy in policies.items():
        all_datasets: list[Dataset] = clis[conn].get_all_datasets()
        path_to_dataset: dict[Path, Dataset] = {d.path: d for d in all_datasets}

        # Resolve dataset paths
        resolved_paths = resolve_paths(
            all_paths=[d.path for d in all_datasets],
            included_exact=policy.include_exact,
            included_recurse=policy.include_recurse,
            excluded_exact=policy.exclude_exact,
            excluded_recurse=policy.exclude_recurse,
            conservative_grouping=strict,
            strict_exclude=strict
        )

        # Ensure there are kept datasets
        if not resolved_paths.kept_paths:
            raise ValueError(f"Resolving datasets for location '{conn}' yielded no datasets")

        # Ensure all explicitly included datasets are kept, to avoid surprises
        for d in policy.include_exact:
            if d and d not in resolved_paths.kept_paths:
                raise ValueError(f"Dataset '{conn}/{d}' is no longer included in resolved datasets")

        # Reconstruct datasets
        resolved_datasets = ResolvedDatasets(
            kept_datasets={path_to_dataset[p] for p in resolved_paths.kept_paths},
            single_datasets={path_to_dataset[p] for p in resolved_paths.single_paths},
            # In ZFS, parents must exist, so this is safe
            recursive_groups={path_to_dataset[p] for p in resolved_paths.recursive_groups}
        )

        datasets[conn] = resolved_datasets

    return datasets, clis

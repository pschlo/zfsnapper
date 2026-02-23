from typing import Optional, Any, TypeGuard
from collections.abc import Collection
from dataclasses import dataclass
from subprocess import CalledProcessError
import logging

from zfsnappr.common.zfs import Snapshot, ZfsCli
from zfsnappr.common.utils import space
from zfsnappr.common.parse_dataset_arg import ConnSpec
from .policy import apply_policy, KeepPolicy
from .grouping import Grouper, apply_grouper, groupers


log = logging.getLogger(__name__)


@dataclass
class GroupInfo[G]:
    groupkey: G
    """The key of a specific group."""
    grouper: Grouper[G]
    """Grouper that was used to create the group."""


def prune_snapshots[G](
    cli: ZfsCli,
    snapshots: Collection[Snapshot],
    policy: KeepPolicy,
    *,
    conn: ConnSpec,
    grouper: Grouper[G] = groupers.NOGROUP,
    dry_run: bool,
    allow_destroy_all: bool = False
) -> None:
    """
    Prune given snapshots according to keep policy
    """
    # Logging
    num_datasets = len({s.dataset for s in snapshots})
    if grouper is groupers.NOGROUP:
        log.info(f'[{conn}] Pruning {len(snapshots)} snapshots on {num_datasets} datasets without grouping')
    else:
        log.info(f'[{conn}] Pruning {len(snapshots)} snapshots on {num_datasets} datasets, grouped by {grouper.name}')

    # Apply policy
    policy_result: dict[G, tuple[list[Snapshot], list[Snapshot]]] = {}
    groups = apply_grouper(snapshots, grouper)
    for groupkey, group in groups.items():
        policy_result[groupkey] = apply_policy(group, policy)

    # Print policy result
    print_policy_result(policy_result, grouper=grouper)

    # Checks
    for groupkey, (keep, destroy) in policy_result.items():
        if not keep and not allow_destroy_all:
            if grouper is groupers.NOGROUP:
                msg = "Refusing to destroy all snapshots"
            else:
                msg = f"Refusing to destroy all snapshots for group: {groupkey}"
            raise RuntimeError(msg)
    total_destroy = {s for (keep, destroy) in policy_result.values() for s in destroy}
    if not total_destroy:
        log.info(space(1) + "No snapshots to destroy")
        return
    if dry_run:
        log.info(space(1) + "Dry-run enabled, not destroying any snapshots")
        return

    # Perform deletions
    log.info(space(1) + f'Destroying {len(total_destroy)} snapshots on {len({s.dataset for s in total_destroy})} datasets')
    _num_destroyed, _num_skipped = 0, 0
    for snap in total_destroy:
        try:
            cli.destroy_snapshots(snap.dataset, [snap.shortname])
            _num_destroyed += 1
        except CalledProcessError:
            log.warning(space(2) + f"Failed to destroy snapshot: {snap.shortname}")
            _num_skipped += 1
        log.info(space(2) + f"{_num_destroyed}/{len(total_destroy)} destroyed ({_num_skipped} skipped)")


def is_nogroup(
    policy_result: dict[Any, tuple[list[Snapshot], list[Snapshot]]],
    g: Grouper[Any]
) -> TypeGuard[dict[None, tuple[list[Snapshot], list[Snapshot]]]]:
    return g is groupers.NOGROUP and len(policy_result) == 1 and None in policy_result


def print_policy_result[G](policy_result: dict[G, tuple[list[Snapshot], list[Snapshot]]], grouper: Grouper[G]):
    if is_nogroup(policy_result, grouper):
        _print_group(*policy_result[None], indent=1)
    else:
        for groupkey, (keep, destroy) in policy_result.items():
            log.info(space(1) + f"{grouper.name.capitalize()}: {groupkey}")
            _print_group(keep, destroy, indent=2)

def _print_group(keep: list[Snapshot], destroy: list[Snapshot], indent: int):
    log.info(space(indent) + f"Keep {len(keep)}")
    if destroy:
        log.info(space(indent) + f"Destroy {len(destroy)}:")
        for snap in destroy:
            log.info(space(indent+1) + f'{snap.timestamp}  {snap.shortname}')
    else:
        log.info(space(indent) + f"Destroy 0")

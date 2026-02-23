from typing import Optional, Any
from collections.abc import Collection
from dataclasses import dataclass
from subprocess import CalledProcessError
import logging

from zfsnappr.common.zfs import Snapshot, ZfsCli
from zfsnappr.common.utils import space
from zfsnappr.common.parse_dataset_arg import ConnSpec
from .policy import apply_policy, KeepPolicy
from .grouping import Grouper, apply_grouper


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
    grouper: Grouper[G] | None,
    dry_run: bool,
    allow_destroy_all: bool = False
) -> None:
    """
    Prune given snapshots according to keep policy
    """
    num_datasets = len({s.dataset for s in snapshots})
    if grouper is None:
        log.info(f'[{conn}] Pruning {len(snapshots)} snapshots on {num_datasets} datasets without grouping')
        keep, destroy = apply_policy(snapshots, policy)
        print_policy_result(keep, destroy, group=None)
    else:
        log.info(f'[{conn}] Pruning {len(snapshots)} snapshots on {num_datasets} datasets, grouped by {grouper.name}')
        # group the snapshots. Result is a dict with group name as key and set of snaps as value
        groups = apply_grouper(snapshots, grouper)
        keep: list[Snapshot] = []
        destroy: list[Snapshot] = []
        for groupkey, group_snaps in groups.items():
            _keep, _destroy = apply_policy(group_snaps, policy)
            keep += _keep
            destroy += _destroy
            print_policy_result(_keep, _destroy, group=GroupInfo(groupkey, grouper))

    if not keep and not allow_destroy_all:
        raise RuntimeError(f"Refusing to destroy all snapshots")
    if not destroy:
        log.info(space(1) + "No snapshots to destroy")
        return
    if dry_run:
        log.info(space(1) + "Dry-run enabled, not destroying any snapshots")
        return

    log.info(space(1) + f'Destroying {len(destroy)} snapshots on {len({s.dataset for s in destroy})} datasets')
    _num_destroyed, _num_skipped = 0, 0
    for snap in destroy:
        try:
            cli.destroy_snapshots(snap.dataset, [snap.shortname])
            _num_destroyed += 1
        except CalledProcessError:
            log.warning(space(2) + f"Failed to destroy snapshot: {snap.shortname}")
            _num_skipped += 1
        log.info(space(2) + f"{_num_destroyed}/{len(destroy)} destroyed ({_num_skipped} skipped)")


def print_policy_result[G](keep: Collection[Snapshot], destroy: Collection[Snapshot], *, group: GroupInfo[G] | None):
    # Determine prefix
    if group is not None:
        log.info(space(1) + f"{group.grouper.name.capitalize()}: {group.groupkey}")

    # Print message
    _i = 2 if group is not None else 1
    log.info(space(_i) + f"Keep {len(keep)}")
    if destroy:
        log.info(space(_i) + f"Destroy {len(destroy)}:")
        for snap in destroy:
            log.info(space(_i+1) + f'{snap.timestamp}  {snap.shortname}')
    else:
        log.info(space(_i) + f"Destroy 0")

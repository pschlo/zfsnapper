from typing import Optional, Any
from collections.abc import Collection
from dataclasses import dataclass
from subprocess import CalledProcessError
import logging

from zfsnappr.common.zfs import Snapshot, ZfsCli
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
    grouper: Grouper[G] | None,
    dry_run: bool,
    allow_destroy_all: bool = False
) -> None:
    """
    Prune given snapshots according to keep policy
    """
    if grouper is None:
        log.info(f'Pruning {len(snapshots)} snapshots without grouping')
        keep, destroy = apply_policy(snapshots, policy)
        print_policy_result(keep, destroy, group=None)
    else:
        log.info(f'Pruning {len(snapshots)} snapshots, grouped by {grouper.name}')
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
        log.info("No snapshots to prune")
        return
    if dry_run:
        log.info("Dry-run enabled, not destroying any snapshots")
        return

    log.info(f'Destroying...')
    _num_destroyed, _num_skipped = 0, 0
    for snap in destroy:
        try:
            cli.destroy_snapshots(snap.dataset, [snap.shortname])
            _num_destroyed += 1
        except CalledProcessError:
            log.warning(f"Failed to destroy snapshot '{snap.shortname}' on '{snap.dataset}'")
            _num_skipped += 1
        log.info(f"    {_num_destroyed}/{len(destroy)} destroyed ({_num_skipped} skipped)")


def print_policy_result[G](keep: Collection[Snapshot], destroy: Collection[Snapshot], *, group: GroupInfo[G] | None):
    # Determine prefix
    if group is None:
        prefix = ""
    else:
        prefix = f"{group.grouper.name.capitalize()} '{group.groupkey}': "

    # Print message
    if not destroy:
        log.info(
            prefix + f'Keeping all {len(keep)} snapshots, not destroying any snapshots'
        )
    else:
        log.info(
            prefix + f'Keeping {len(keep)} snapshots, destroying these {len(destroy)} snapshots:'
        )
        for snap in destroy:
            log.info(f'    {snap.timestamp}  {snap.longname}')

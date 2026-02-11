from __future__ import annotations
from collections.abc import Collection
import logging

from ..zfs import Snapshot, ZfsCli
from ..utils import group_by
from .replicate_snaps import replicate_snaps
from zfsnappr.common.exception import ReplicationError


log = logging.getLogger(__name__)


def replicate_hierarchy(
    source_cli: ZfsCli, source_dataset_root: str, source_snaps: Collection[Snapshot],
    dest_cli: ZfsCli, dest_dataset_root: str,
    existing_dest_datasets: Collection[str],
    initialize: bool,
    rollback: bool,
):
  """
  replicates given snaps under dest_dataset
  keeps the dataset hierarchy
  all source_snaps must be under source_dataset_root
  """
  is_error: bool = False

  # Group by absolute source dataset name
  grouped = group_by(source_snaps, lambda s: s.dataset)

  src_ds_rootparts = source_dataset_root.split('/')
  dest_ds_rootparts = dest_dataset_root.split('/')
  assert all(src_ds_rootparts) and all(dest_ds_rootparts)

  # Ensure parents come before children (stable, explicit ordering).
  # Sort by dataset depth relative to root (then lexicographically for determinism).
  def _rel_parts(abs_ds: str) -> list[str]:
    parts = abs_ds.split('/')
    assert all(parts)
    assert parts[:len(src_ds_rootparts)] == src_ds_rootparts
    return parts[len(src_ds_rootparts):]

  def _depth(abs_ds: str) -> int:
    return len(_rel_parts(abs_ds))

  ordered_source_datasets = sorted(grouped.keys(), key=lambda ds: (_depth(ds), ds))

  for abs_source_dataset in ordered_source_datasets:
    snaps_for_dataset = grouped[abs_source_dataset]
    relparts = _rel_parts(abs_source_dataset)
    abs_dest_dataset = '/'.join(dest_ds_rootparts + relparts)

    try:
      replicate_snaps(
        source_cli=source_cli,
        source_snaps=snaps_for_dataset,
        dest_cli=dest_cli,
        dest_dataset=abs_dest_dataset,
        existing_dest_datasets=existing_dest_datasets,
        initialize=initialize,
        rollback=rollback,
      )
    except ReplicationError as e:
      is_error = True
      log.error(e)

  if is_error:
    raise ReplicationError(f"Replication failed for one or more datasets")

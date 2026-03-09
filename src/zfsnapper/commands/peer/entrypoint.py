from __future__ import annotations
from typing import cast, Optional, TYPE_CHECKING
import logging
from collections.abc import Collection

from zfsnapper.common.zfs import ZfsProperty, ZfsCli, Dataset, Snapshot
from zfsnapper.common.resolve_datasets import ResolvedDatasets
from zfsnapper.common.command_utils import fetch_snaps, resolve_dataset_args, resolve_filter_args
from zfsnapper.common.filter import SnapFilter
from zfsnapper.common.parse_dataset_arg import ConnSpec
if TYPE_CHECKING:
    from .args import Args

from . import list as _list, prune as _prune


log = logging.getLogger(__name__)


def entrypoint(args: Args):
    s = args.peer_command
    args.__delattr__("peer_command")

    match s:
        case 'list':
            _list.entrypoint(cast(_list.Args, args))
        case 'prune':
            _prune.entrypoint(cast(_prune.Args, args))
        case _:
            assert False

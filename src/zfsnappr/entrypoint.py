#!/usr/bin/env python3

from __future__ import annotations
from typing import cast
import sys
import logging

from .setup_logging import setup_logging
setup_logging()
from .args import get_args
from .commands import (
  prune as _prune,
  create as _create,
  push as _push,
  list as _list,
  tag as _tag,
  unhold as _unhold,
  prune_peers as _prune_peers,
  version as _version
)

log = logging.getLogger(__name__)


def cli():
    try:
        _entrypoint()
    except Exception as e:
        log.error(e)
        # raise
        sys.exit(1)


def _entrypoint():
    args = get_args()
    s = args.subcommand
    args.__delattr__("subcommand")

    match s:
        case 'prune':
            _prune.entrypoint(cast(_prune.Args, args))
        case 'create':
            _create.entrypoint(cast(_create.Args, args))
        case 'push':
            _push.entrypoint(cast(_push.Args, args))
        case 'list':
            _list.entrypoint(cast(_list.Args, args))
        case 'tag':
            _tag.entrypoint(cast(_tag.Args, args))
        case 'unhold':
            _unhold.entrypoint(cast(_unhold.Args, args))
        case 'prune-peers':
            _prune_peers.entrypoint(cast(_prune_peers.Args, args))
        case 'version':
            _version.entrypoint(cast(_version.Args, args))
        case _:
            assert False

from __future__ import annotations
from dataclasses import dataclass
from typing import Optional, Literal
from argparse import ArgumentParser

from zfsnapper.common.args import CommonArgs
from .replicate import EncryptionMode


class Args(CommonArgs):
    dest: str
    init: bool
    rollback: bool
    enc_mode: EncryptionMode
    localhost: str | None


def setup(parser: ArgumentParser) -> None:
    parser.add_argument('dest', metavar='USER@HOST:PORT::DATASET')
    parser.add_argument('--init', action='store_true')
    parser.add_argument('--rollback', action='store_true')
    parser.add_argument(
        '--encryption',
        '--enc',
        dest='enc_mode',
        type=EncryptionMode,
        choices=list(EncryptionMode),
        default=EncryptionMode.KEEP,
        help=(
            "Encryption handling: "
            "'keep' = preserve source encryption when relevant (raw send); "
            "'clear' = use plain send"
        ),
    )
    parser.add_argument('--localhost', type=str)

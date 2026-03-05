from __future__ import annotations
from typing import Optional, Callable, cast
from dataclasses import dataclass
import logging

from zfsnappr.common.zfs import Snapshot


log = logging.getLogger(__name__)


COLUMN_SEPARATOR = ' | '
HEADER_SEPARATOR = '-'

@dataclass
class Field:
    name: str
    get: Callable[[Snapshot], str]
    # whether to blank this column on wrapped lines
    blank_on_wrap: bool = False


def render_table(fields: list[Field], snaps: list[Snapshot]) -> None:
    headers = [f.name for f in fields]

    # rows_blocks[row][col] = list of lines
    rows_blocks: list[list[list[str]]] = [
        [cell_lines(f.get(snap)) for f in fields]
        for snap in snaps
    ]

    # widths from the max visible line length in each column (including header)
    widths: list[int] = []
    for col, f in enumerate(fields):
        max_cell = 0
        for row in rows_blocks:
            max_cell = max(max_cell, max(len(line) for line in row[col]))
        widths.append(max(len(headers[col]), max_cell))

    total_width = (len(COLUMN_SEPARATOR) * (len(fields) - 1)) + sum(widths)

    # header
    log.info(COLUMN_SEPARATOR.join(h.ljust(w) for h, w in zip(headers, widths)))
    log.info((HEADER_SEPARATOR * (total_width // len(HEADER_SEPARATOR) + 1))[:total_width])

    # body
    for row in rows_blocks:
        row_height = max(len(cell) for cell in row)

        for i in range(row_height):
            parts: list[str] = []
            for col, f in enumerate(fields):
                cell = row[col]
                line = cell[i] if i < len(cell) else ""

                # blank columns on wrapped lines if requested
                if i > 0 and f.blank_on_wrap:
                    line = ""

                parts.append(line.ljust(widths[col]))

            log.info(COLUMN_SEPARATOR.join(parts))


def cell_lines(text: str) -> list[str]:
    # keep it simple; you can also handle \r\n etc if needed
    return text.splitlines() or [""]

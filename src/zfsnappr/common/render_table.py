from __future__ import annotations
from typing import Optional, Callable, cast, Unpack, TypeVarTuple
from collections.abc import Collection, Sequence
from dataclasses import dataclass
import logging

from zfsnappr.common.zfs import Snapshot


log = logging.getLogger(__name__)


COLUMN_SEPARATOR = ' | '
HEADER_SEPARATOR = '-'

Ts = TypeVarTuple("Ts")


@dataclass
class Field[*Ts]:
    name: str
    get: Callable[[Unpack[Ts]], str]
    # whether to blank this column on wrapped lines
    blank_on_wrap: bool = False


def render_table[*Ts](
    fields: list[Field[*Ts]],
    data: Collection[tuple[*Ts]],
    column_separators: Sequence[str] | None = None,
    header_column_separators: Sequence[str] | None = None,
) -> None:
    headers = [f.name for f in fields]

    if column_separators is None:
        column_separators = [COLUMN_SEPARATOR] * (len(fields) - 1)
    elif len(column_separators) != max(0, len(fields) - 1):
        raise ValueError(
            f"column_separators must have exactly {len(fields) - 1} entries "
            f"(got {len(column_separators)})"
        )
    
    if header_column_separators is None:
        header_column_separators = column_separators
    elif len(header_column_separators) != max(0, len(fields) - 1):
        raise ValueError(
            f"header_column_separators must have exactly {len(fields) - 1} entries "
            f"(got {len(header_column_separators)})"
        )

    # rows_blocks[row][col] = list of lines
    rows_blocks: list[list[list[str]]] = [
        [cell_lines(f.get(*row_data)) for f in fields]
        for row_data in data
    ]

    # widths from the max visible line length in each column (including header)
    widths: list[int] = []
    for col, f in enumerate(fields):
        max_cell = 0
        for row in rows_blocks:
            max_cell = max(max_cell, max(len(line) for line in row[col]))
        widths.append(max(len(headers[col]), max_cell))

    total_width = sum(widths) + sum(len(sep) for sep in column_separators)

    def join_columns(parts: Sequence[str], separators: Sequence[str]) -> str:
        if not parts:
            return ""
        out = [parts[0]]
        for sep, part in zip(separators, parts[1:]):
            out.append(sep)
            out.append(part)
        return "".join(out)

    # header
    log.info(join_columns([h.ljust(w) for h, w in zip(headers, widths)], header_column_separators))
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

            log.info(join_columns(parts, column_separators))


def cell_lines(text: str) -> list[str]:
    # keep it simple; you can also handle \r\n etc if needed
    return text.splitlines() or [""]

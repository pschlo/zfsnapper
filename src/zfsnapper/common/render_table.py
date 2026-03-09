from __future__ import annotations
from typing import Callable, Literal, Unpack, TypeVarTuple, cast
from collections.abc import Collection, Sequence
from dataclasses import dataclass
import logging

from zfsnapper.common.zfs import Snapshot


log = logging.getLogger(__name__)


COLUMN_SEPARATOR = " │ "
HEADER_SEPARATOR = "─"

Ts = TypeVarTuple("Ts")
SeparatorMode = Literal["always", "both", "either"]
Alignment = Literal["left", "right"]


@dataclass
class Field[*Ts]:
    name: str
    get: Callable[[Unpack[Ts]], str]
    # whether to blank this column on wrapped lines
    blank_on_wrap: bool = False
    align: Alignment = "left"
    header_align: Alignment | None = None


def render_table[*Ts](
    fields: list[Field[*Ts]],
    data: Collection[tuple[*Ts]],
    column_separators: Sequence[str] | None = None,
    header_column_separators: Sequence[str] | None = None,
    column_separator_modes: Sequence[SeparatorMode] | None = None,
) -> None:
    headers = [f.name for f in fields]
    separator_count = max(0, len(fields) - 1)

    if column_separators is None:
        column_separators = [COLUMN_SEPARATOR] * separator_count
    elif len(column_separators) != separator_count:
        raise ValueError(
            f"column_separators must have exactly {separator_count} entries "
            f"(got {len(column_separators)})"
        )

    if header_column_separators is None:
        header_column_separators = column_separators
    elif len(header_column_separators) != separator_count:
        raise ValueError(
            f"header_column_separators must have exactly {separator_count} entries "
            f"(got {len(header_column_separators)})"
        )

    if column_separator_modes is None:
        column_separator_modes = cast(list[SeparatorMode], ["always"] * separator_count)
    elif len(column_separator_modes) != separator_count:
        raise ValueError(
            f"column_separator_modes must have exactly {separator_count} entries "
            f"(got {len(column_separator_modes)})"
        )

    invalid_modes = [m for m in column_separator_modes if m not in {"always", "both", "either"}]
    if invalid_modes:
        raise ValueError(f"invalid column separator modes: {invalid_modes!r}")

    invalid_alignments = [
        f.align for f in fields if f.align not in {"left", "right"}
    ]
    if invalid_alignments:
        raise ValueError(f"invalid field alignments: {invalid_alignments!r}")

    invalid_header_alignments = [
        f.header_align for f in fields
        if f.header_align is not None and f.header_align not in {"left", "right"}
    ]
    if invalid_header_alignments:
        raise ValueError(f"invalid field header_align values: {invalid_header_alignments!r}")

    # rows_blocks[row][col] = list of lines
    rows_blocks: list[list[list[str]]] = [
        [cell_lines(f.get(*row_data)) for f in fields]
        for row_data in data
    ]

    # widths from the max visible line length in each column (including header)
    widths: list[int] = []
    for col in range(len(fields)):
        max_cell = 0
        for row in rows_blocks:
            max_cell = max(max_cell, max(len(line) for line in row[col]))
        widths.append(max(len(headers[col]), max_cell))

    total_width = sum(widths) + sum(len(sep) for sep in column_separators)

    def justify(text: str, width: int, align: Alignment) -> str:
        if align == "left":
            return text.ljust(width)
        if align == "right":
            return text.rjust(width)
        raise ValueError(f"invalid alignment: {align!r}")

    def join_columns(
        parts: Sequence[str],
        separators: Sequence[str],
        raw_parts: Sequence[str] | None = None,
        separator_modes: Sequence[SeparatorMode] | None = None,
    ) -> str:
        if not parts:
            return ""

        if raw_parts is None:
            raw_parts = parts

        if separator_modes is None:
            separator_modes = cast(list[SeparatorMode], ["always"] * len(separators))

        out = [parts[0]]
        for i, (sep, part, mode) in enumerate(zip(separators, parts[1:], separator_modes), start=1):
            left_raw = raw_parts[i - 1]
            right_raw = raw_parts[i]

            if mode == "always":
                rendered_sep = sep
            elif mode == "both":
                rendered_sep = sep if (left_raw != "" and right_raw != "") else " " * len(sep)
            elif mode == "either":
                rendered_sep = sep if (left_raw != "" or right_raw != "") else " " * len(sep)
            else:
                raise ValueError(f"invalid column separator mode: {mode!r}")

            out.append(rendered_sep)
            out.append(part)

        return "".join(out)

    # header
    header_parts = [
        justify(h, w, f.header_align if f.header_align is not None else f.align)
        for h, w, f in zip(headers, widths, fields)
    ]
    log.info(
        join_columns(
            header_parts,
            header_column_separators,
        )
    )
    log.info((HEADER_SEPARATOR * (total_width // len(HEADER_SEPARATOR) + 1))[:total_width])

    # body
    for row in rows_blocks:
        row_height = max(len(cell) for cell in row)

        for i in range(row_height):
            raw_parts: list[str] = []
            padded_parts: list[str] = []

            for col, f in enumerate(fields):
                cell = row[col]
                line = cell[i] if i < len(cell) else ""

                # blank columns on wrapped lines if requested
                if i > 0 and f.blank_on_wrap:
                    line = ""

                raw_parts.append(line)
                padded_parts.append(justify(line, widths[col], f.align))

            log.info(
                join_columns(
                    padded_parts,
                    column_separators,
                    raw_parts=raw_parts,
                    separator_modes=column_separator_modes,
                )
            )


def cell_lines(text: str) -> list[str]:
    # keep it simple; you can also handle \r\n etc if needed
    return text.splitlines() or [""]

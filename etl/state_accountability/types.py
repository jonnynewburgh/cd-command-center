"""Shared types for the state accountability ETL framework."""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Literal, Protocol

import pandas as pd


# Re-export FileType from the TN parser; future state parsers should define
# their own FileType enums in their state subpackage.
LoadStatus = Literal["success", "failed", "partial", "skipped"]


Severity = Literal["error", "warning"]


@dataclass(frozen=True)
class ValidationFailure:
    """One row failing one validator rule.

    severity='error' → ROLLBACK the load (data is wrong, don't commit).
    severity='warning' → log + record in notes, but COMMIT (known artifact,
    or a regression catcher that shouldn't block a load that's otherwise good).
    """
    rule: str
    message: str
    severity: Severity = "error"
    context: dict[str, Any] = field(default_factory=dict)


class ValidationError(Exception):
    """Raised inside the load transaction when validators return error-severity
    failures. Triggers ROLLBACK; the failed attempt is then recorded in a fresh
    transaction by the runner.

    Warning-severity failures do NOT raise this — the runner inspects them
    separately and threads them into the load row's notes on COMMIT.
    """
    def __init__(self, failures: list[ValidationFailure]):
        self.failures = failures
        super().__init__(
            f"{len(failures)} validation error(s): "
            + "; ".join(f"[{f.rule}] {f.message}" for f in failures[:3])
            + (f" ...and {len(failures) - 3} more" if len(failures) > 3 else "")
        )


@dataclass(frozen=True)
class LoadResult:
    """Outcome of a single file load attempt."""
    filepath: Path
    status: LoadStatus | Literal["already_loaded"]
    load_id: int | None = None
    row_counts: dict[str, int] = field(default_factory=dict)
    reason: str | None = None
    failures: list[ValidationFailure] = field(default_factory=list)


# --- Handler protocol ---

# A handler turns a raw source file into a {table_name: rows} dict ready for INSERT.
# Each row is a dict whose keys match the destination table's columns (excluding
# source_load_id, which the runner injects).

ParseFn = Callable[[Path], pd.DataFrame]
TransformFn = Callable[[pd.DataFrame, int], dict[str, list[dict]]]


@dataclass(frozen=True)
class FileHandler:
    """A full handler: parse + transform. Loading is generic (runner does it)."""
    parse: ParseFn
    transform: TransformFn
    target_tables: tuple[str, ...]  # for documentation/auditing


@dataclass(frozen=True)
class SkipHandler:
    """A no-op handler that logs status='skipped' and exits cleanly.

    Use when the parser recognizes a file type that we deliberately don't load
    (e.g. district TVAAS files for which we have no destination table).
    """
    reason: str

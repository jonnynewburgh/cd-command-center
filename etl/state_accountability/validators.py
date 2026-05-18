"""Validator framework.

Validators are pure functions of (cursor, load_id) -> list[ValidationFailure].
They run inside the load transaction, after fact rows have been INSERTed but
before COMMIT. Any failure raises ValidationError, which triggers ROLLBACK.

Per-state validator registries live in each state subpackage's validators.py
(e.g. etl/state_accountability/tn/validators.py). The runner imports the
registry for the state being loaded and calls run_validators().
"""
from __future__ import annotations

from typing import Callable

from .types import ValidationFailure

Validator = Callable[[object, int], list[ValidationFailure]]
"""(cursor, load_id) -> failures. cursor is a psycopg2 cursor; typed as object
to avoid importing psycopg2 here."""


def run_validators(cur, load_id: int, validators: list[Validator]) -> list[ValidationFailure]:
    """Run every validator, return the concatenated failures.

    All validators run (no short-circuit) so a single load attempt surfaces
    every issue. The caller (runner) splits the result by severity:
      - error-severity failures → raise ValidationError → ROLLBACK
      - warning-severity failures → log + thread into the load row's notes,
        but do NOT abort the COMMIT
    """
    failures: list[ValidationFailure] = []
    for v in validators:
        failures.extend(v(cur, load_id))
    return failures


def split_by_severity(
    failures: list[ValidationFailure],
) -> tuple[list[ValidationFailure], list[ValidationFailure]]:
    """Return (errors, warnings)."""
    errors = [f for f in failures if f.severity == "error"]
    warnings = [f for f in failures if f.severity == "warning"]
    return errors, warnings

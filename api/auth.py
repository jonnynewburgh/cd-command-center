"""
api/auth.py — Shared-secret write-token guard.

Interim protection for write routes (notes, bookmarks) until real
per-user auth is added. Pairs with the scoped CORS allowlist in
api/main.py:

  - CORS blocks third-party browser pages from driving writes.
  - This token blocks anonymous curl/scripts hitting the public URL.

Set API_WRITE_TOKEN in the deploy environment (Render → env vars).
If the env var is unset, the guard logs a warning at request time and
allows the write (so local dev keeps working). Treat the unset state as
a misconfiguration in prod, not a feature.
"""

import logging
import os
import secrets

from fastapi import Header, HTTPException, status

logger = logging.getLogger(__name__)

_HEADER_NAME = "X-API-Token"
_WARNED_UNSET = False


def require_write_token(x_api_token: str | None = Header(default=None)) -> None:
    """FastAPI dependency that gates write routes on a shared secret."""
    global _WARNED_UNSET
    expected = os.environ.get("API_WRITE_TOKEN", "").strip()

    if not expected:
        if not _WARNED_UNSET:
            logger.warning(
                "API_WRITE_TOKEN is unset — write routes are unauthenticated. "
                "Set this env var in production."
            )
            _WARNED_UNSET = True
        return

    if not x_api_token or not secrets.compare_digest(x_api_token, expected):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing or invalid API token",
            headers={"WWW-Authenticate": _HEADER_NAME},
        )

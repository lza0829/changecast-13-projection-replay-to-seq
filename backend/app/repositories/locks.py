"""Per-stream operation locks.

Every operation that writes the **main projection** (a regular run, a replay
promotion) or builds a replay snapshot for a stream must first acquire that
stream's lock. Replays do not write the main projection, but they still take
the lock so a regular run cannot start mid-replay and vice versa.

Locks are rows keyed by ``stream_id``; acquiring is an INSERT that fails on
conflict. A short TTL makes a crashed holder's lock reclaimable without manual
intervention — the holder still releases it explicitly on the happy path.
"""

from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta, timezone

LOCK_TTL_SECONDS = 120


class StreamBusyError(Exception):
    """Raised when the stream lock is already held by another operation."""

    def __init__(self, stream_id: int, holder: dict | None = None):
        self.stream_id = stream_id
        self.holder = holder
        op = holder["operation"] if holder else "unknown"
        ref = holder["holder_ref"] if holder else "unknown"
        super().__init__(
            f"stream {stream_id} is locked by {op} ({ref})"
        )


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _fmt(dt: datetime) -> str:
    return dt.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _parse(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def acquire_lock(
    conn: sqlite3.Connection,
    *,
    stream_id: int,
    operation: str,
    holder_ref: str,
) -> None:
    """Acquire the stream lock, expiring any stale TTL lock first.

    Raises :class:`StreamBusyError` if a live lock is held by someone else.
    """
    now = _now()
    row = conn.execute(
        "SELECT * FROM stream_locks WHERE stream_id=?", (stream_id,)
    ).fetchone()
    if row is not None:
        if _parse(row["expires_at"]) > now:
            raise StreamBusyError(stream_id, holder=dict(row))
        # Stale lock from a crashed holder — reclaim it.
        conn.execute("DELETE FROM stream_locks WHERE stream_id=?", (stream_id,))

    conn.execute(
        """
        INSERT INTO stream_locks(stream_id, operation, holder_ref, acquired_at, expires_at)
        VALUES (?,?,?,?,?)
        """,
        (
            stream_id,
            operation,
            holder_ref,
            _fmt(now),
            _fmt(now + timedelta(seconds=LOCK_TTL_SECONDS)),
        ),
    )


def release_lock(
    conn: sqlite3.Connection, *, stream_id: int, holder_ref: str
) -> None:
    """Release the lock if held by ``holder_ref`` (never unlock someone else)."""
    conn.execute(
        "DELETE FROM stream_locks WHERE stream_id=? AND holder_ref=?",
        (stream_id, holder_ref),
    )


def list_locks(conn: sqlite3.Connection) -> list[dict]:
    now = _now()
    out = []
    for r in conn.execute("SELECT * FROM stream_locks ORDER BY stream_id").fetchall():
        d = dict(r)
        d["live"] = _parse(d["expires_at"]) > now
        out.append(d)
    return out

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from app.config import FIXTURES_DIR
from app.db import connect
from app.engines.project import apply_events
from app.repositories import batches as batch_repo
from app.repositories import locks as lock_repo
from app.repositories import projections as proj_repo
from app.repositories import streams as stream_repo
from app.repositories.locks import StreamBusyError

__all__ = ["StreamBusyError"]


def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _load_fixture(name: str) -> list[dict[str, Any]]:
    path = FIXTURES_DIR / name
    with path.open(encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, list):
        raise ValueError(f"fixture {name} must be a JSON array")
    return data


def dashboard_stats() -> dict[str, Any]:
    conn = connect()
    try:
        return {
            "stream_count": conn.execute("SELECT COUNT(*) c FROM project_streams").fetchone()["c"],
            "enabled_streams": conn.execute(
                "SELECT COUNT(*) c FROM project_streams WHERE enabled=1"
            ).fetchone()["c"],
            "batch_count": conn.execute("SELECT COUNT(*) c FROM project_batches").fetchone()["c"],
            "projection_alive": conn.execute(
                "SELECT COUNT(*) c FROM projection_rows WHERE deleted=0"
            ).fetchone()["c"],
            "projection_deleted": conn.execute(
                "SELECT COUNT(*) c FROM projection_rows WHERE deleted=1"
            ).fetchone()["c"],
            "ooo_buffered": conn.execute("SELECT COUNT(*) c FROM ooo_buffer").fetchone()["c"],
            "replay_count": conn.execute("SELECT COUNT(*) c FROM replay_runs").fetchone()["c"],
            "replay_failed": conn.execute(
                "SELECT COUNT(*) c FROM replay_runs WHERE status='failed'"
            ).fetchone()["c"],
            "locked_streams": conn.execute("SELECT COUNT(*) c FROM stream_locks").fetchone()["c"],
            "apply_order": conn.execute(
                "SELECT value FROM settings WHERE key='apply_order'"
            ).fetchone()["value"],
        }
    finally:
        conn.close()


def list_streams() -> list[dict[str, Any]]:
    conn = connect()
    try:
        return stream_repo.list_streams(conn)
    finally:
        conn.close()


def list_batches(limit: int = 50) -> list[dict[str, Any]]:
    conn = connect()
    try:
        rows = batch_repo.list_batches(conn, limit=limit)
        streams = {s["id"]: s for s in stream_repo.list_streams(conn)}
        for r in rows:
            s = streams.get(r["stream_id"])
            r["stream_code"] = s["code"] if s else None
            r["stream_title"] = s["title"] if s else None
        return rows
    finally:
        conn.close()


def list_projections(stream_id: int | None = None, include_deleted: bool = True) -> list[dict[str, Any]]:
    conn = connect()
    try:
        rows = proj_repo.list_projections(
            conn, stream_id=stream_id, include_deleted=include_deleted
        )
        streams = {s["id"]: s for s in stream_repo.list_streams(conn)}
        for r in rows:
            s = streams.get(r["stream_id"])
            r["stream_code"] = s["code"] if s else None
            r["stream_title"] = s["title"] if s else None
        return rows
    finally:
        conn.close()


def list_events(batch_id: int | None = None, stream_id: int | None = None) -> list[dict[str, Any]]:
    conn = connect()
    try:
        return batch_repo.list_applied_events(conn, batch_id=batch_id, stream_id=stream_id)
    finally:
        conn.close()


def list_ooo_buffer(stream_id: int | None = None) -> list[dict[str, Any]]:
    conn = connect()
    try:
        return proj_repo.list_ooo_buffer(conn, stream_id=stream_id)
    finally:
        conn.close()


def list_settings() -> dict[str, str]:
    conn = connect()
    try:
        rows = conn.execute("SELECT key, value FROM settings").fetchall()
        return {r["key"]: r["value"] for r in rows}
    finally:
        conn.close()


def run_project(
    stream_id: int,
    note: str | None = None,
    hold_seconds: float = 0,
) -> dict[str, Any]:
    """Load CDC fixture, apply by seq, replace projection for the stream.

    Takes the per-stream lock for the whole write window. A replay or another
    run holding the lock gets a :class:`StreamBusyError` (HTTP 409) — regular
    runs and replays are mutually exclusive per stream.
    """
    import time

    conn = connect()
    holder_ref = f"run-{_now()}"
    try:
        stream = stream_repo.get_stream(conn, stream_id)
        if not stream:
            raise ValueError("stream not found")
        if not stream["enabled"]:
            raise ValueError("stream disabled")

        # Main-projection write: must own the stream lock.
        lock_repo.acquire_lock(
            conn,
            stream_id=stream_id,
            operation="run",
            holder_ref=holder_ref,
        )
        conn.commit()

        if hold_seconds > 0:
            # Demo/test hook: hold the lock with the write window open so a
            # concurrent replay/run hits the mutual-exclusion guard.
            time.sleep(hold_seconds)

        events = _load_fixture(stream["fixture"])
        # Base 0-1: each run rebuilds projection from the full fixture by seq.
        # Incremental watermark / cross-run idempotency left for later topics.
        result = apply_events(events, known_event_ids=set())
        now = _now()
        seq = conn.execute("SELECT COUNT(*) c FROM project_batches").fetchone()["c"] + 1
        batch_no = f"PB-{now[:10].replace('-', '')}-{seq:04d}"
        batch_id = batch_repo.insert_batch(
            conn,
            batch_no=batch_no,
            stream_id=stream_id,
            status="completed",
            counts=result["counts"],
            note=note,
            created_at=now,
        )
        proj_repo.replace_stream_projection(
            conn, stream_id=stream_id, rows=result["projection"], updated_at=now
        )
        for ev in result["applied_log"]:
            batch_repo.insert_applied_event(
                conn, batch_id=batch_id, stream_id=stream_id, event=ev, created_at=now
            )
        stream_repo.set_watermark(conn, stream_id, result["counts"]["max_seq"])
        conn.commit()

        batch = batch_repo.get_batch(conn, batch_id)
        assert batch is not None
        batch["stream_code"] = stream["code"]
        batch["stream_title"] = stream["title"]
        batch["apply_order"] = "by_seq"
        batch["counts"] = result["counts"]
        batch["projection_preview"] = proj_repo.list_projections(conn, stream_id=stream_id)[:20]
        batch["event_preview"] = batch_repo.list_applied_events(conn, batch_id=batch_id, limit=20)
        return batch
    finally:
        try:
            lock_repo.release_lock(conn, stream_id=stream_id, holder_ref=holder_ref)
            conn.commit()
        finally:
            conn.close()

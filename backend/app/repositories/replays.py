"""Persistence for replay snapshots — isolated replay_* tables only.

Nothing in this module touches ``projection_rows``, ``project_batches`` or
``project_streams.watermark_seq``. Promotion into the main projection lives in
the service layer and goes through its own explicit interface.
"""

from __future__ import annotations

import json
import sqlite3
from typing import Any


def insert_replay_run(
    conn: sqlite3.Connection,
    *,
    replay_no: str,
    stream_id: int,
    target_seq: int,
    status: str,
    batch_size: int,
    created_at: str,
) -> int:
    cur = conn.execute(
        """
        INSERT INTO replay_runs(
            replay_no, stream_id, target_seq, status,
            source_event_count, source_max_seq, source_truncated,
            batch_count, batch_size,
            rows_total, rows_alive, rows_deleted, snapshot_max_seq,
            checks_json, error, created_at
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            replay_no,
            stream_id,
            target_seq,
            status,
            0, 0, 0, 0, batch_size, 0, 0, 0, 0, None, None, created_at,
        ),
    )
    return int(cur.lastrowid)


def mark_replay_failed(
    conn: sqlite3.Connection, *, replay_id: int, error: str, finished_at: str
) -> None:
    conn.execute(
        "UPDATE replay_runs SET status='failed', error=?, finished_at=? WHERE id=?",
        (error, finished_at, replay_id),
    )


def complete_replay_run(
    conn: sqlite3.Connection,
    *,
    replay_id: int,
    source_event_count: int,
    source_max_seq: int,
    source_truncated: bool,
    batch_count: int,
    rows_total: int,
    rows_alive: int,
    rows_deleted: int,
    snapshot_max_seq: int,
    checks: dict[str, Any],
    finished_at: str,
) -> None:
    conn.execute(
        """
        UPDATE replay_runs SET
            status='completed',
            source_event_count=?, source_max_seq=?, source_truncated=?,
            batch_count=?, rows_total=?, rows_alive=?, rows_deleted=?,
            snapshot_max_seq=?, checks_json=?, finished_at=?
        WHERE id=?
        """,
        (
            source_event_count,
            source_max_seq,
            1 if source_truncated else 0,
            batch_count,
            rows_total,
            rows_alive,
            rows_deleted,
            snapshot_max_seq,
            json.dumps(checks, ensure_ascii=False),
            finished_at,
            replay_id,
        ),
    )


def insert_replay_batch(
    conn: sqlite3.Connection,
    *,
    replay_id: int,
    batch: dict[str, Any],
) -> int:
    c = batch["counts"]
    cur = conn.execute(
        """
        INSERT INTO replay_batches(
            replay_id, batch_index, seq_from, seq_to,
            source_events, applied_upsert, applied_delete, skipped_duplicate,
            projected_alive, projected_deleted, max_seq
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            replay_id,
            batch["batch_index"],
            batch["seq_from"],
            batch["seq_to"],
            c["source_events"],
            c["applied_upsert"],
            c["applied_delete"],
            c["skipped_duplicate"],
            c["projected_alive"],
            c["projected_deleted"],
            c["max_seq"],
        ),
    )
    return int(cur.lastrowid)


def replace_replay_projection(
    conn: sqlite3.Connection,
    *,
    replay_id: int,
    stream_id: int,
    rows: list[dict[str, Any]],
) -> None:
    conn.execute("DELETE FROM replay_projection_rows WHERE replay_id=?", (replay_id,))
    for row in rows:
        conn.execute(
            """
            INSERT INTO replay_projection_rows(
                replay_id, stream_id, entity_key, payload_json,
                applied_seq, applied_event_id, deleted
            ) VALUES (?,?,?,?,?,?,?)
            """,
            (
                replay_id,
                stream_id,
                row["entity_key"],
                json.dumps(row.get("payload"), ensure_ascii=False)
                if row.get("payload") is not None
                else None,
                row["applied_seq"],
                row["applied_event_id"],
                1 if row.get("deleted") else 0,
            ),
        )


def insert_replay_event(
    conn: sqlite3.Connection,
    *,
    replay_id: int,
    replay_batch_id: int,
    stream_id: int,
    event: dict[str, Any],
) -> None:
    conn.execute(
        """
        INSERT INTO replay_applied_events(
            replay_id, replay_batch_id, stream_id,
            event_id, entity_key, op, seq, event_time, result
        ) VALUES (?,?,?,?,?,?,?,?,?)
        """,
        (
            replay_id,
            replay_batch_id,
            stream_id,
            event["event_id"],
            event["entity_key"],
            event["op"],
            event["seq"],
            event["event_time"],
            event["result"],
        ),
    )


def list_replay_runs(
    conn: sqlite3.Connection, *, stream_id: int | None = None, limit: int = 50
) -> list[dict[str, Any]]:
    sql = "SELECT * FROM replay_runs WHERE 1=1"
    params: list[Any] = []
    if stream_id is not None:
        sql += " AND stream_id=?"
        params.append(stream_id)
    sql += " ORDER BY id DESC LIMIT ?"
    params.append(limit)
    return [_hydrate_run(dict(r)) for r in conn.execute(sql, params).fetchall()]


def get_replay_run(conn: sqlite3.Connection, replay_id: int) -> dict[str, Any] | None:
    r = conn.execute(
        "SELECT * FROM replay_runs WHERE id=?", (replay_id,)
    ).fetchone()
    return _hydrate_run(dict(r)) if r else None


def list_replay_batches(conn: sqlite3.Connection, replay_id: int) -> list[dict]:
    rows = conn.execute(
        "SELECT * FROM replay_batches WHERE replay_id=? ORDER BY batch_index",
        (replay_id,),
    ).fetchall()
    return [dict(r) for r in rows]


def list_replay_rows(
    conn: sqlite3.Connection, replay_id: int, include_deleted: bool = True
) -> list[dict[str, Any]]:
    sql = "SELECT * FROM replay_projection_rows WHERE replay_id=?"
    params: list[Any] = [replay_id]
    if not include_deleted:
        sql += " AND deleted=0"
    sql += " ORDER BY entity_key"
    out = []
    for r in conn.execute(sql, params).fetchall():
        d = dict(r)
        d["payload"] = json.loads(d.pop("payload_json")) if d["payload_json"] else None
        d["deleted"] = bool(d["deleted"])
        out.append(d)
    return out


def mark_promoted(
    conn: sqlite3.Connection,
    *,
    replay_id: int,
    batch_id: int,
    promoted_at: str,
) -> None:
    conn.execute(
        """
        UPDATE replay_runs
        SET status='promoted', promoted_at=?, promoted_batch_id=?
        WHERE id=?
        """,
        (promoted_at, batch_id, replay_id),
    )


def _hydrate_run(d: dict[str, Any]) -> dict[str, Any]:
    d["source_truncated"] = bool(d["source_truncated"])
    d["checks"] = json.loads(d.pop("checks_json")) if d.get("checks_json") else None
    return d

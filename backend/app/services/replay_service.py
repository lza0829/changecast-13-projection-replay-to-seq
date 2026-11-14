"""Target-seq replay service.

A replay rebuilds a snapshot by replaying the **event source** up to a target
seq and stores it in isolated ``replay_*`` tables. It never rewinds or writes
the main watermark, never deletes/updates ``projection_rows`` and never inserts
into ``project_batches``. The only path from a replay snapshot into the main
projection is :func:`promote_replay`, a separate interface that re-runs the
cross-checks and verifies the truncation point before swapping.

Concurrency: replay, regular run and promotion all take the same per-stream
lock (see ``repositories.locks``), so per stream they are mutually exclusive.
"""

from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Any

from app.db import connect
from app.engines.crosscheck import cross_check
from app.engines.replay import replay_to_seq
from app.repositories import batches as batch_repo
from app.repositories import locks as lock_repo
from app.repositories import projections as proj_repo
from app.repositories import replays as replay_repo
from app.repositories import streams as stream_repo
from app.repositories.locks import StreamBusyError

DEFAULT_BATCH_SIZE = 2
MAX_HOLD_SECONDS = 10

__all__ = ["StreamBusyError"]


class ReplayError(ValueError):
    """A replay precondition or cross-check failure."""


def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _load_fixture(name: str) -> list[dict[str, Any]]:
    import json

    from app.config import FIXTURES_DIR

    with (FIXTURES_DIR / name).open(encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, list):
        raise ValueError(f"fixture {name} must be a JSON array")
    return data


def start_replay(
    stream_id: int,
    target_seq: int,
    *,
    batch_size: int = DEFAULT_BATCH_SIZE,
    hold_seconds: float = 0,
    force_fail: bool = False,
) -> dict[str, Any]:
    """Replay the stream's event source up to ``target_seq`` (isolated)."""
    if target_seq < 0:
        raise ReplayError("target_seq must be >= 0")
    if batch_size < 1:
        raise ReplayError("batch_size must be >= 1")
    hold_seconds = max(0.0, min(float(hold_seconds), MAX_HOLD_SECONDS))

    conn = connect()
    holder_ref = f"replay-{_now()}"
    replay_id: int | None = None
    try:
        stream = stream_repo.get_stream(conn, stream_id)
        if not stream:
            raise ReplayError("stream not found")
        if not stream["enabled"]:
            raise ReplayError("stream disabled")

        source_events = _load_fixture(stream["fixture"])
        source_max = max((int(e.get("seq", 0)) for e in source_events), default=0)
        if target_seq > source_max:
            raise ReplayError(
                f"target_seq {target_seq} is beyond the source truncation point "
                f"(source max seq {source_max})"
            )

        # Replay holds the same per-stream lock as a regular run.
        lock_repo.acquire_lock(
            conn,
            stream_id=stream_id,
            operation="replay",
            holder_ref=holder_ref,
        )
        now = _now()
        n = conn.execute("SELECT COUNT(*) c FROM replay_runs").fetchone()["c"] + 1
        replay_no = f"RP-{now[:10].replace('-', '')}-{n:04d}"
        replay_id = replay_repo.insert_replay_run(
            conn,
            replay_no=replay_no,
            stream_id=stream_id,
            target_seq=target_seq,
            status="running",
            batch_size=batch_size,
            created_at=now,
        )
        conn.commit()

        if hold_seconds > 0:
            # Demo/test hook: keep the lock held so a concurrent run/replay on
            # the same stream is rejected with 409. No main table is touched.
            time.sleep(hold_seconds)

        if force_fail:
            # Demo hook: simulate a replay blow-up. Everything so far lives in
            # replay_* tables only; the main projection stays untouched.
            err = "forced replay failure (demo)"
            replay_repo.mark_replay_failed(
                conn, replay_id=replay_id, error=err, finished_at=_now()
            )
            conn.commit()
            raise ReplayError(err)

        try:
            replay = replay_to_seq(
                source_events, target_seq=target_seq, batch_size=batch_size
            )
            report = cross_check(
                source_events=source_events,
                target_seq=target_seq,
                replay=replay,
            )
        except Exception as exc:  # engine failure must not poison the main side
            replay_repo.mark_replay_failed(
                conn, replay_id=replay_id, error=str(exc), finished_at=_now()
            )
            conn.commit()
            raise ReplayError(f"replay failed: {exc}") from exc

        if not report["ok"]:
            err = "cross-check failed: " + "; ".join(
                c["name"] for c in report["checks"] if not c["ok"]
            )
            replay_repo.mark_replay_failed(
                conn, replay_id=replay_id, error=err, finished_at=_now()
            )
            conn.commit()
            raise ReplayError(err)

        # Persist the isolated snapshot, batches and event trace.
        for batch in replay["batches"]:
            rb_id = replay_repo.insert_replay_batch(
                conn, replay_id=replay_id, batch=batch
            )
            for ev in batch["applied_log"]:
                replay_repo.insert_replay_event(
                    conn,
                    replay_id=replay_id,
                    replay_batch_id=rb_id,
                    stream_id=stream_id,
                    event=ev,
                )
        replay_repo.replace_replay_projection(
            conn,
            replay_id=replay_id,
            stream_id=stream_id,
            rows=replay["snapshot"],
        )
        replay_repo.complete_replay_run(
            conn,
            replay_id=replay_id,
            source_event_count=report["source_event_count"],
            source_max_seq=report["source_max_seq"],
            source_truncated=report["source_truncated"],
            batch_count=report["batch_count"],
            rows_total=report["snapshot"]["rows_total"],
            rows_alive=report["snapshot"]["rows_alive"],
            rows_deleted=report["snapshot"]["rows_deleted"],
            snapshot_max_seq=report["snapshot"]["max_seq"],
            checks=report,
            finished_at=_now(),
        )
        conn.commit()

        return get_replay(replay_id)
    finally:
        try:
            lock_repo.release_lock(
                conn, stream_id=stream_id, holder_ref=holder_ref
            )
            conn.commit()
        finally:
            conn.close()


def list_replays(stream_id: int | None = None, limit: int = 50) -> list[dict[str, Any]]:
    conn = connect()
    try:
        rows = replay_repo.list_replay_runs(conn, stream_id=stream_id, limit=limit)
        streams = {s["id"]: s for s in stream_repo.list_streams(conn)}
        for r in rows:
            s = streams.get(r["stream_id"])
            r["stream_code"] = s["code"] if s else None
            r["stream_title"] = s["title"] if s else None
        return rows
    finally:
        conn.close()


def get_replay(replay_id: int) -> dict[str, Any]:
    conn = connect()
    try:
        run = replay_repo.get_replay_run(conn, replay_id)
        if not run:
            raise ReplayError("replay not found")
        stream = stream_repo.get_stream(conn, run["stream_id"])
        run["stream_code"] = stream["code"] if stream else None
        run["stream_title"] = stream["title"] if stream else None
        run["batches"] = replay_repo.list_replay_batches(conn, replay_id)
        run["rows"] = replay_repo.list_replay_rows(conn, replay_id)
        run["locks"] = lock_repo.list_locks(conn)
        return run
    finally:
        conn.close()


def replay_diff(replay_id: int) -> dict[str, Any]:
    """Compare an isolated replay snapshot against the current main projection."""
    conn = connect()
    try:
        run = replay_repo.get_replay_run(conn, replay_id)
        if not run:
            raise ReplayError("replay not found")
        if run["status"] not in ("completed", "promoted"):
            raise ReplayError(
                f"replay is {run['status']}; only completed replays can be diffed"
            )

        replay_rows = {
            r["entity_key"]: r
            for r in replay_repo.list_replay_rows(conn, replay_id)
        }
        main_rows = {
            r["entity_key"]: r
            for r in proj_repo.list_projections(conn, stream_id=run["stream_id"])
        }
        stream = stream_repo.get_stream(conn, run["stream_id"])

        only_replay, only_main, changed = [], [], []
        for key in sorted(set(replay_rows) | set(main_rows)):
            r, m = replay_rows.get(key), main_rows.get(key)
            if m is None:
                only_replay.append(_row_brief(r))
            elif r is None:
                only_main.append(_row_brief(m))
            elif (
                r["applied_seq"] != m["applied_seq"]
                or bool(r["deleted"]) != bool(m["deleted"])
                or r["payload"] != m["payload"]
            ):
                changed.append(
                    {
                        "entity_key": key,
                        "replay": _row_brief(r),
                        "main": _row_brief(m),
                    }
                )

        return {
            "replay_id": replay_id,
            "stream_id": run["stream_id"],
            "stream_code": stream["code"] if stream else None,
            "target_seq": run["target_seq"],
            "replay_max_seq": run["snapshot_max_seq"],
            "main_watermark_seq": stream["watermark_seq"] if stream else None,
            "replay_rows": len(replay_rows),
            "main_rows": len(main_rows),
            "only_in_replay": only_replay,
            "only_in_main": only_main,
            "changed": changed,
            "identical": not (only_replay or only_main or changed),
        }
    finally:
        conn.close()


def promote_replay(replay_id: int) -> dict[str, Any]:
    """Promote a verified replay snapshot into the main projection.

    Separate, explicit interface. Re-runs the cross-checks against the stored
    snapshot and refuses unless the snapshot's truncation point is internally
    consistent. A successful replay on its own never moves the watermark.
    """
    conn = connect()
    holder_ref = f"promote-{_now()}"
    run = None
    try:
        run = replay_repo.get_replay_run(conn, replay_id)
        if not run:
            raise ReplayError("replay not found")
        if run["status"] != "completed":
            raise ReplayError(
                f"only completed replays can be promoted (current: {run['status']})"
            )

        stream_id = run["stream_id"]
        lock_repo.acquire_lock(
            conn,
            stream_id=stream_id,
            operation="promote",
            holder_ref=holder_ref,
        )
        conn.commit()

        # Re-verify truncation point against the *persisted* snapshot.
        stored_rows = replay_repo.list_replay_rows(conn, replay_id)
        stored_max = max((r["applied_seq"] for r in stored_rows), default=0)
        stored_batches = replay_repo.list_replay_batches(conn, replay_id)
        checks = run["checks"] or {}
        problems = []
        if not checks.get("ok"):
            problems.append("stored cross-check report is not ok")
        if stored_max != run["snapshot_max_seq"]:
            problems.append("stored snapshot max seq disagrees with replay record")
        if stored_max > run["target_seq"]:
            problems.append("snapshot contains events beyond the target truncation point")
        if len(stored_rows) != run["rows_total"]:
            problems.append("stored row count disagrees with replay record")
        if sum(b["source_events"] for b in stored_batches) != run["source_event_count"]:
            problems.append("stored batch counts do not reconcile to truncation point")
        if problems:
            raise ReplayError("promotion refused: " + "; ".join(problems))

        stream = stream_repo.get_stream(conn, stream_id)
        assert stream is not None
        now = _now()
        n = conn.execute("SELECT COUNT(*) c FROM project_batches").fetchone()["c"] + 1
        batch_no = f"PB-{now[:10].replace('-', '')}-{n:04d}"

        # Build main-side rows from the verified isolated snapshot.
        main_rows = [
            {
                "entity_key": r["entity_key"],
                "payload": r["payload"],
                "applied_seq": r["applied_seq"],
                "applied_event_id": r["applied_event_id"],
                "deleted": bool(r["deleted"]),
            }
            for r in stored_rows
        ]
        alive = sum(1 for r in main_rows if not r["deleted"])
        deleted = len(main_rows) - alive
        upserts = sum(1 for r in main_rows if not r["deleted"])
        deletes = sum(1 for r in main_rows if r["deleted"])
        counts = {
            "source_events": run["source_event_count"],
            "applied_upsert": upserts,
            "applied_delete": deletes,
            "skipped_duplicate": 0,
            "projected_alive": alive,
            "projected_deleted": deleted,
            "max_seq": stored_max,
        }
        batch_id = batch_repo.insert_batch(
            conn,
            batch_no=batch_no,
            stream_id=stream_id,
            status="promoted",
            counts=counts,
            note=f"promoted from replay {run['replay_no']} @seq {run['target_seq']}",
            created_at=now,
        )
        # Main projection swap + the ONLY watermark move outside a regular run.
        proj_repo.replace_stream_projection(
            conn, stream_id=stream_id, rows=main_rows, updated_at=now
        )
        for ev in conn.execute(
            """
            SELECT event_id, entity_key, op, seq, event_time, result
            FROM replay_applied_events WHERE replay_id=? ORDER BY id
            """,
            (replay_id,),
        ).fetchall():
            batch_repo.insert_applied_event(
                conn,
                batch_id=batch_id,
                stream_id=stream_id,
                event=dict(ev),
                created_at=now,
            )
        stream_repo.set_watermark(conn, stream_id, stored_max)
        replay_repo.mark_promoted(
            conn, replay_id=replay_id, batch_id=batch_id, promoted_at=now
        )
        conn.commit()

        return {
            "replay_id": replay_id,
            "promoted_batch_id": batch_id,
            "batch_no": batch_no,
            "stream_id": stream_id,
            "watermark_seq": stored_max,
            "rows_total": len(main_rows),
            "rows_alive": alive,
            "rows_deleted": deleted,
        }
    finally:
        try:
            if run is not None:
                lock_repo.release_lock(
                    conn, stream_id=run["stream_id"], holder_ref=holder_ref
                )
                conn.commit()
        finally:
            conn.close()


def list_locks() -> list[dict[str, Any]]:
    conn = connect()
    try:
        return lock_repo.list_locks(conn)
    finally:
        conn.close()


def _row_brief(r: dict[str, Any]) -> dict[str, Any]:
    return {
        "entity_key": r["entity_key"],
        "applied_seq": r["applied_seq"],
        "applied_event_id": r["applied_event_id"],
        "deleted": bool(r["deleted"]),
        "payload": r["payload"],
    }

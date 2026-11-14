"""Replay a CDC event source up to a target seq, in explicit batches.

Unlike the regular projection path, a replay:

* reads the **event source** (the full fixture) and truncates at ``seq <= target_seq``
  instead of touching the main watermark;
* applies the truncated source in numbered batches while carrying projection
  state and the known-event-id set across batches, so every batch's counters
  are real and the batch totals can be cross-checked against the source
  truncation point and the final snapshot;
* never writes the main projection — persistence of the returned rows/logs is
  the caller's responsibility and goes into isolated replay tables.
"""

from __future__ import annotations

from typing import Any

from app.engines.project import apply_events


def truncate_source(
    events: list[dict[str, Any]], target_seq: int
) -> list[dict[str, Any]]:
    """Return source events at or below ``target_seq`` (the truncation point)."""
    return [ev for ev in events if int(ev.get("seq", 0)) <= target_seq]


def _apply_chunk(
    chunk: list[dict[str, Any]],
    state: dict[str, dict[str, Any]],
    known: set[str],
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """Apply one batch on top of carried cumulative state."""
    counts = {
        "source_events": len(chunk),
        "applied_upsert": 0,
        "applied_delete": 0,
        "skipped_duplicate": 0,
        "max_seq": 0,
        "projected_alive": 0,
        "projected_deleted": 0,
    }
    applied_log: list[dict[str, Any]] = []

    for ev in chunk:
        event_id = str(ev["event_id"])
        entity_key = str(ev["entity_key"])
        op = str(ev.get("op", "upsert"))
        seq = int(ev.get("seq", 0))
        event_time = str(ev.get("event_time", ""))
        counts["max_seq"] = max(counts["max_seq"], seq)

        if event_id in known:
            counts["skipped_duplicate"] += 1
            applied_log.append(
                {
                    "event_id": event_id,
                    "entity_key": entity_key,
                    "op": op,
                    "seq": seq,
                    "event_time": event_time,
                    "result": "skipped_duplicate",
                    "payload": ev.get("payload"),
                }
            )
            continue

        known.add(event_id)
        if op == "delete":
            state[entity_key] = {
                "entity_key": entity_key,
                "payload": None,
                "applied_seq": seq,
                "applied_event_id": event_id,
                "deleted": True,
            }
            counts["applied_delete"] += 1
            result = "applied_delete"
        else:
            state[entity_key] = {
                "entity_key": entity_key,
                "payload": ev.get("payload") or {},
                "applied_seq": seq,
                "applied_event_id": event_id,
                "deleted": False,
            }
            counts["applied_upsert"] += 1
            result = "applied_upsert"

        applied_log.append(
            {
                "event_id": event_id,
                "entity_key": entity_key,
                "op": op,
                "seq": seq,
                "event_time": event_time,
                "result": result,
                "payload": ev.get("payload"),
            }
        )

    counts["projected_alive"] = sum(1 for r in state.values() if not r["deleted"])
    counts["projected_deleted"] = sum(1 for r in state.values() if r["deleted"])
    return counts, applied_log


def replay_to_seq(
    events: list[dict[str, Any]],
    *,
    target_seq: int,
    batch_size: int = 2,
) -> dict[str, Any]:
    """Replay ``events`` truncated at ``target_seq`` in batches of ``batch_size``.

    The truncated source is sorted by seq (mirroring the base apply order) and
    sliced into batches. Each batch is applied on top of the cumulative state,
    so cross-batch duplicates are detected and per-batch counters describe the
    state held after that batch.
    """
    if target_seq < 0:
        raise ValueError("target_seq must be >= 0")
    if batch_size < 1:
        raise ValueError("batch_size must be >= 1")

    truncated = truncate_source(events, target_seq)
    ordered = sorted(
        truncated, key=lambda e: (int(e.get("seq", 0)), str(e.get("event_id", "")))
    )

    known_event_ids: set[str] = set()
    state: dict[str, dict[str, Any]] = {}
    batches: list[dict[str, Any]] = []
    totals = {
        "source_events": 0,
        "applied_upsert": 0,
        "applied_delete": 0,
        "skipped_duplicate": 0,
        "max_seq": 0,
    }

    for index, start in enumerate(range(0, len(ordered), batch_size), start=1):
        chunk = ordered[start : start + batch_size]
        counts, applied_log = _apply_chunk(chunk, state, known_event_ids)

        totals["source_events"] += counts["source_events"]
        totals["applied_upsert"] += counts["applied_upsert"]
        totals["applied_delete"] += counts["applied_delete"]
        totals["skipped_duplicate"] += counts["skipped_duplicate"]
        totals["max_seq"] = max(totals["max_seq"], counts["max_seq"])

        batches.append(
            {
                "batch_index": index,
                "seq_from": int(chunk[0].get("seq", 0)),
                "seq_to": int(chunk[-1].get("seq", 0)),
                "counts": counts,
                "applied_log": applied_log,
            }
        )

    # Independent recomputation over the whole truncated source via the base
    # engine; the batch path and this full-pass snapshot must reconcile.
    final = apply_events(ordered, known_event_ids=set())
    snapshot = final["projection"]
    snapshot_counts = final["counts"]

    return {
        "target_seq": target_seq,
        "truncated_count": len(ordered),
        "source_max_seq": max((int(e.get("seq", 0)) for e in ordered), default=0),
        "batch_size": batch_size,
        "batches": batches,
        "batch_totals": totals,
        "snapshot": snapshot,
        "snapshot_counts": snapshot_counts,
    }

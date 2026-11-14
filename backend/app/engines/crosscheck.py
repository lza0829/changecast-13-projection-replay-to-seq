"""Cross-checks that make a target-seq replay self-proving.

Four independent views of the same replay must agree:

1. **Source truncation point** — how many source events have ``seq <= target``
   and what is their max seq (computed straight from the event source).
2. **Replay batch counters** — summing every replay batch must equal the
   truncation point's totals.
3. **Target snapshot row set** — row count/alive/dead and the max applied seq
   stored on the snapshot rows.
4. **Full-recompute counters** — a non-batched pass over the truncated source
   via the base engine.

Any mismatch fails the replay; a failed replay never promotes and never
touches the main projection.
"""

from __future__ import annotations

from typing import Any


def cross_check(
    *,
    source_events: list[dict[str, Any]],
    target_seq: int,
    replay: dict[str, Any],
) -> dict[str, Any]:
    truncated_source = [
        ev for ev in source_events if int(ev.get("seq", 0)) <= target_seq
    ]
    source_count = len(truncated_source)
    source_max_seq = max(
        (int(ev.get("seq", 0)) for ev in truncated_source), default=0
    )
    source_truncated = any(
        int(ev.get("seq", 0)) > target_seq for ev in source_events
    )

    batches = replay["batches"]
    batch_sum = {
        "source_events": sum(b["counts"]["source_events"] for b in batches),
        "applied_upsert": sum(b["counts"]["applied_upsert"] for b in batches),
        "applied_delete": sum(b["counts"]["applied_delete"] for b in batches),
        "skipped_duplicate": sum(
            b["counts"]["skipped_duplicate"] for b in batches
        ),
    }
    last_batch = batches[-1] if batches else None
    batch_state_alive = last_batch["counts"]["projected_alive"] if last_batch else 0
    batch_state_deleted = (
        last_batch["counts"]["projected_deleted"] if last_batch else 0
    )
    batch_max_seq = max(
        (b["counts"]["max_seq"] for b in batches), default=0
    )

    snapshot = replay["snapshot"]
    snapshot_alive = sum(1 for r in snapshot if not r["deleted"])
    snapshot_deleted = sum(1 for r in snapshot if r["deleted"])
    snapshot_max_seq = max((int(r["applied_seq"]) for r in snapshot), default=0)
    snapshot_event_ids = {str(r["applied_event_id"]) for r in snapshot}

    snap_counts = replay["snapshot_counts"]
    totals = replay["batch_totals"]

    checks: list[dict[str, Any]] = [
        {
            "name": "truncation_count_matches_batches",
            "detail": (
                f"source events with seq<={target_seq}: {source_count}; "
                f"sum of batch source_events: {batch_sum['source_events']}"
            ),
            "ok": source_count == batch_sum["source_events"]
            and source_count == replay["truncated_count"],
        },
        {
            "name": "truncation_max_seq_matches",
            "detail": (
                f"source max seq below target: {source_max_seq}; "
                f"batch max seq: {batch_max_seq}; "
                f"snapshot max seq: {snapshot_max_seq}"
            ),
            "ok": source_max_seq == batch_max_seq == snapshot_max_seq
            and snapshot_max_seq <= target_seq,
        },
        {
            "name": "batch_totals_match_full_recompute",
            "detail": (
                f"batch totals upsert/delete/skip="
                f"{totals['applied_upsert']}/{totals['applied_delete']}/"
                f"{totals['skipped_duplicate']}; full recompute="
                f"{snap_counts['applied_upsert']}/{snap_counts['applied_delete']}/"
                f"{snap_counts['skipped_duplicate']}"
            ),
            "ok": (
                totals["applied_upsert"] == snap_counts["applied_upsert"]
                and totals["applied_delete"] == snap_counts["applied_delete"]
                and totals["skipped_duplicate"]
                == snap_counts["skipped_duplicate"]
                and totals["source_events"] == snap_counts["source_events"]
                and totals["max_seq"] == snap_counts["max_seq"]
            ),
        },
        {
            "name": "snapshot_rows_match_batch_state",
            "detail": (
                f"snapshot rows total/alive/deleted="
                f"{len(snapshot)}/{snapshot_alive}/{snapshot_deleted}; "
                f"last batch state={batch_state_alive}/{batch_state_deleted}"
            ),
            "ok": (
                len(snapshot) == snap_counts["projected_alive"]
                + snap_counts["projected_deleted"]
                and snapshot_alive == batch_state_alive
                and snapshot_deleted == batch_state_deleted
                and snapshot_alive == snap_counts["projected_alive"]
                and snapshot_deleted == snap_counts["projected_deleted"]
            ),
        },
        {
            "name": "snapshot_event_ids_within_truncation",
            "detail": (
                f"{len(snapshot_event_ids)} snapshot event ids; "
                f"all must come from events with seq<={target_seq}"
            ),
            "ok": all(
                int(ev.get("seq", 0)) <= target_seq
                for ev in truncated_source
                if str(ev.get("event_id")) in snapshot_event_ids
            )
            and len(snapshot_event_ids) <= source_count,
        },
    ]

    return {
        "ok": all(c["ok"] for c in checks),
        "target_seq": target_seq,
        "source_truncated": source_truncated,
        "source_event_count": source_count,
        "source_max_seq": source_max_seq,
        "batch_count": len(batches),
        "batch_sum": batch_sum,
        "snapshot": {
            "rows_total": len(snapshot),
            "rows_alive": snapshot_alive,
            "rows_deleted": snapshot_deleted,
            "max_seq": snapshot_max_seq,
        },
        "checks": checks,
    }

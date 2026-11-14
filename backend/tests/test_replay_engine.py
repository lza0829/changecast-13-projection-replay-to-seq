"""Engine-level replay + cross-check tests (no DB)."""

from __future__ import annotations

import pytest

from app.engines.crosscheck import cross_check
from app.engines.project import apply_events
from app.engines.replay import replay_to_seq, truncate_source

EVENTS = [
    {"event_id": "e1", "entity_key": "A", "op": "upsert", "seq": 1,
     "event_time": "t1", "payload": {"v": 1}},
    {"event_id": "e2", "entity_key": "B", "op": "upsert", "seq": 2,
     "event_time": "t2", "payload": {"v": 2}},
    {"event_id": "e3", "entity_key": "A", "op": "upsert", "seq": 3,
     "event_time": "t3", "payload": {"v": 3}},
    {"event_id": "e4", "entity_key": "B", "op": "delete", "seq": 4,
     "event_time": "t4", "payload": None},
    {"event_id": "e5", "entity_key": "C", "op": "upsert", "seq": 5,
     "event_time": "t5", "payload": {"v": 5}},
]


def test_truncate_source_filters_above_target():
    assert [e["event_id"] for e in truncate_source(EVENTS, 3)] == ["e1", "e2", "e3"]
    assert truncate_source(EVENTS, 0) == []
    assert len(truncate_source(EVENTS, 10)) == 5


def test_replay_snapshot_equals_plain_apply_of_truncated_source():
    replay = replay_to_seq(EVENTS, target_seq=3, batch_size=2)
    plain = apply_events(truncate_source(EVENTS, 3))
    assert replay["snapshot"] == plain["projection"]
    assert replay["truncated_count"] == 3
    assert replay["source_max_seq"] == 3
    assert len(replay["batches"]) == 2


def test_batches_cover_every_truncated_event_and_carry_state():
    replay = replay_to_seq(EVENTS, target_seq=4, batch_size=2)
    # 4 events / size 2 -> exactly two batches covering seq 1-2 and 3-4
    assert [(b["seq_from"], b["seq_to"]) for b in replay["batches"]] == [
        (1, 2), (3, 4)
    ]
    assert sum(b["counts"]["source_events"] for b in replay["batches"]) == 4
    # The delete of B happens in batch 2; last batch state must show it.
    last = replay["batches"][-1]["counts"]
    assert last["projected_alive"] == 1  # only A remains alive
    assert last["projected_deleted"] == 1  # B tombstone


def test_batch_totals_reconcile_to_full_recompute():
    replay = replay_to_seq(EVENTS, target_seq=5, batch_size=2)
    t, s = replay["batch_totals"], replay["snapshot_counts"]
    assert t["applied_upsert"] == s["applied_upsert"] == 4
    assert t["applied_delete"] == s["applied_delete"] == 1
    assert t["source_events"] == s["source_events"] == 5
    assert t["max_seq"] == s["max_seq"] == 5


def test_cross_check_passes_on_consistent_replay():
    replay = replay_to_seq(EVENTS, target_seq=4, batch_size=2)
    report = cross_check(source_events=EVENTS, target_seq=4, replay=replay)
    assert report["ok"]
    assert report["source_truncated"] is True
    assert report["source_event_count"] == 4
    assert report["snapshot"]["max_seq"] == 4
    assert all(c["ok"] for c in report["checks"])


def test_cross_check_fails_when_snapshot_tampered_beyond_target():
    replay = replay_to_seq(EVENTS, target_seq=3, batch_size=2)
    # Tamper: inject a row carrying a seq beyond the truncation point.
    replay["snapshot"].append(
        {
            "entity_key": "X", "payload": {"v": 9}, "applied_seq": 5,
            "applied_event_id": "e5", "deleted": False,
        }
    )
    report = cross_check(source_events=EVENTS, target_seq=3, replay=replay)
    assert not report["ok"]
    failed = {c["name"] for c in report["checks"] if not c["ok"]}
    assert "truncation_max_seq_matches" in failed
    assert "snapshot_rows_match_batch_state" in failed


def test_replay_to_zero_seq_is_empty_snapshot():
    replay = replay_to_seq(EVENTS, target_seq=0, batch_size=2)
    assert replay["snapshot"] == []
    assert replay["batches"] == []
    report = cross_check(source_events=EVENTS, target_seq=0, replay=replay)
    assert report["ok"]
    assert report["snapshot"]["max_seq"] == 0


def test_replay_rejects_bad_arguments():
    with pytest.raises(ValueError):
        replay_to_seq(EVENTS, target_seq=-1)
    with pytest.raises(ValueError):
        replay_to_seq(EVENTS, target_seq=3, batch_size=0)

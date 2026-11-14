"""Service-level tests: isolation, mutual exclusion, promotion, failure safety."""

from __future__ import annotations

import concurrent.futures
import time

import pytest

from app.repositories.locks import StreamBusyError
from app.services import project_service, replay_service


def _wait_for_lock(stream_id: int, timeout: float = 5.0) -> dict:
    """Poll until the stream actually shows a live lock (no fixed-sleep race)."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        for lock in replay_service.list_locks():
            if lock["stream_id"] == stream_id and lock["live"]:
                return lock
        time.sleep(0.02)
    raise AssertionError(f"stream {stream_id} lock was never acquired")


@pytest.fixture()
def base_run(fresh_db):
    # Stream 1 (member_profile, 6 source events) projected to the full source.
    project_service.run_project(1, note="base")
    return project_service.list_streams()[0]


def test_replay_stores_isolated_snapshot_and_keeps_main_untouched(fresh_db, base_run):
    main_before = project_service.list_projections(stream_id=1)
    wm_before = base_run["watermark_seq"]
    assert wm_before == 6

    run = replay_service.start_replay(1, 3, batch_size=2)
    assert run["status"] == "completed"
    assert run["target_seq"] == 3
    assert run["source_truncated"] is True
    assert run["snapshot_max_seq"] == 3
    assert run["rows_total"] == 2  # M1001 (seq3) + M1002 (seq2, still alive)

    # Main projection and watermark are byte-for-byte unchanged.
    assert project_service.list_projections(stream_id=1) == main_before
    assert project_service.list_streams()[0]["watermark_seq"] == wm_before

    # Replay rows live in the isolated table and carry no ids from main tables.
    assert {r["entity_key"] for r in run["rows"]} == {"M1001", "M1002"}
    assert all(r["applied_seq"] <= 3 for r in run["rows"])


def test_replay_batch_counters_mutually_corroborate(fresh_db, base_run):
    run = replay_service.start_replay(1, 4, batch_size=2)
    batches = run["batches"]
    assert sum(b["source_events"] for b in batches) == run["source_event_count"] == 4
    assert max(b["max_seq"] for b in batches) == run["snapshot_max_seq"] == 4
    assert run["checks"]["ok"]
    # Row set / max seq / batch counts all agree.
    assert len(run["rows"]) == run["rows_total"]
    assert run["rows_alive"] + run["rows_deleted"] == run["rows_total"]


def test_replay_diff_against_main_projection(fresh_db, base_run):
    run = replay_service.start_replay(1, 3, batch_size=2)
    diff = replay_service.replay_diff(run["id"])
    assert diff["identical"] is False
    # M1003 only exists after seq 4; M1002 differs (alive at seq3, deleted at seq5).
    assert {r["entity_key"] for r in diff["only_in_main"]} == {"M1003"}
    assert {c["entity_key"] for c in diff["changed"]} == {"M1002"}
    assert diff["main_watermark_seq"] == 6
    assert diff["replay_max_seq"] == 3


def test_failed_replay_never_poisons_main_projection(fresh_db, base_run):
    main_before = project_service.list_projections(stream_id=1)
    with pytest.raises(replay_service.ReplayError):
        replay_service.start_replay(1, 2, force_fail=True)

    failed = [r for r in replay_service.list_replays(1) if r["status"] == "failed"]
    assert len(failed) == 1 and failed[0]["error"]
    # Main side untouched and no lock left behind.
    assert project_service.list_projections(stream_id=1) == main_before
    assert project_service.list_streams()[0]["watermark_seq"] == 6
    assert replay_service.list_locks() == []
    # A failed replay cannot be promoted.
    with pytest.raises(replay_service.ReplayError):
        replay_service.promote_replay(failed[0]["id"])


def test_target_beyond_source_is_rejected(fresh_db, base_run):
    with pytest.raises(replay_service.ReplayError):
        replay_service.start_replay(1, 999)


def test_run_blocks_replay_on_same_stream(fresh_db, base_run):
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        hold = pool.submit(project_service.run_project, 2, "hold", 2.0)
        _wait_for_lock(2)
        blocked = pool.submit(replay_service.start_replay, 2, 3)
        hold.result()
        with pytest.raises(StreamBusyError):
            blocked.result()
    assert replay_service.list_locks() == []


def test_replay_blocks_run_and_other_replay_on_same_stream(fresh_db, base_run):
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        hold = pool.submit(
            replay_service.start_replay, 2, 3, **{"hold_seconds": 2.0}
        )
        _wait_for_lock(2)
        blocked_run = pool.submit(project_service.run_project, 2, "nope")
        blocked_replay = pool.submit(replay_service.start_replay, 2, 1)
        hold.result()
        with pytest.raises(StreamBusyError):
            blocked_run.result()
        with pytest.raises(StreamBusyError):
            blocked_replay.result()
    assert replay_service.list_locks() == []


def test_lock_is_per_stream_other_stream_keeps_running(fresh_db, base_run):
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        hold = pool.submit(
            replay_service.start_replay, 1, 2, **{"hold_seconds": 1.5}
        )
        _wait_for_lock(1)
        other = pool.submit(replay_service.start_replay, 2, 2)
        hold.result()
        assert other.result()["status"] == "completed"


def test_promotion_is_separate_interface_and_moves_watermark(fresh_db, base_run):
    run = replay_service.start_replay(1, 3, batch_size=2)
    # Merely completing a replay does not move the watermark.
    assert project_service.list_streams()[0]["watermark_seq"] == 6

    result = replay_service.promote_replay(run["id"])
    assert result["watermark_seq"] == 3
    assert project_service.list_streams()[0]["watermark_seq"] == 3

    promoted = replay_service.get_replay(run["id"])
    assert promoted["status"] == "promoted"
    assert promoted["promoted_batch_id"] == result["promoted_batch_id"]

    # Main projection now equals the replay snapshot and diff is identical.
    diff = replay_service.replay_diff(run["id"])
    assert diff["identical"] is True

    # Promotion is one-shot.
    with pytest.raises(replay_service.ReplayError):
        replay_service.promote_replay(run["id"])


def test_promotion_takes_lock_and_is_blocked_by_run(fresh_db, base_run):
    run = replay_service.start_replay(3, 2)
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        hold = pool.submit(project_service.run_project, 3, "hold", 2.0)
        _wait_for_lock(3)
        blocked = pool.submit(replay_service.promote_replay, run["id"])
        hold.result()
        with pytest.raises(StreamBusyError):
            blocked.result()


def test_replay_runs_are_listed_per_stream(fresh_db, base_run):
    replay_service.start_replay(1, 2)
    replay_service.start_replay(1, 4)
    replay_service.start_replay(2, 1)
    assert len(replay_service.list_replays(stream_id=1)) == 2
    assert len(replay_service.list_replays()) == 3

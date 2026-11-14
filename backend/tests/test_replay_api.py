"""HTTP-level tests against a real uvicorn server.

A live server (see ``live_server`` fixture) is required so concurrent requests
actually run on separate threads and contend on the per-stream DB lock.
"""

from __future__ import annotations

import concurrent.futures
import time

import httpx


def _wait_for_lock(client: httpx.Client, stream_id: int, timeout: float = 5.0) -> dict:
    """Poll the lock endpoint until the stream's lock is actually live."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        locks = client.get("/api/stream-locks").json()
        for lock in locks:
            if lock["stream_id"] == stream_id and lock["live"]:
                return lock
        time.sleep(0.02)
    raise AssertionError(f"stream {stream_id} lock was never acquired")


def test_replay_lifecycle_over_http(live_server):
    base = live_server
    with httpx.Client(base_url=base, timeout=10) as c:
        r = c.post("/api/project", json={"stream_id": 1})
        assert r.status_code == 200

        r = c.post(
            "/api/replays",
            json={"stream_id": 1, "target_seq": 3, "batch_size": 2},
        )
        assert r.status_code == 200
        body = r.json()
        replay_id = body["id"]
        assert body["status"] == "completed"
        assert body["checks"]["ok"] is True
        assert body["source_truncated"] is True

        r = c.get(f"/api/replays/{replay_id}")
        assert r.status_code == 200
        assert len(r.json()["batches"]) == body["batch_count"]

        r = c.get(f"/api/replays/{replay_id}/diff")
        assert r.status_code == 200
        assert r.json()["identical"] is False

        r = c.post(f"/api/replays/{replay_id}/promote")
        assert r.status_code == 200
        assert r.json()["watermark_seq"] == 3

        streams = c.get("/api/streams").json()
        assert streams[0]["watermark_seq"] == 3

        # After promotion the replay-vs-main diff is identical.
        r = c.get(f"/api/replays/{replay_id}/diff")
        assert r.json()["identical"] is True


def test_replay_returns_409_when_run_holds_lock(live_server):
    base = live_server
    with httpx.Client(base_url=base, timeout=10) as c:
        c.post("/api/project", json={"stream_id": 1})
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            hold = pool.submit(
                c.post,
                "/api/project",
                json={"stream_id": 2, "hold_seconds": 2.0},
            )
            _wait_for_lock(c, 2)
            blocked = c.post(
                "/api/replays", json={"stream_id": 2, "target_seq": 2}
            )
            assert hold.result().status_code == 200
        assert blocked.status_code == 409
        detail = blocked.json()["detail"]
        assert detail["error"] == "stream_locked"
        assert detail["holder"]["operation"] == "run"
        assert c.get("/api/stream-locks").json() == []


def test_run_returns_409_when_replay_holds_lock(live_server):
    base = live_server
    with httpx.Client(base_url=base, timeout=10) as c:
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            hold = pool.submit(
                c.post,
                "/api/replays",
                json={"stream_id": 1, "target_seq": 2, "hold_seconds": 2.0},
            )
            _wait_for_lock(c, 1)
            blocked = c.post("/api/project", json={"stream_id": 1})
            assert hold.result().status_code == 200
        assert blocked.status_code == 409
        assert blocked.json()["detail"]["holder"]["operation"] == "replay"


def test_promote_returns_409_when_run_holds_lock(live_server):
    base = live_server
    with httpx.Client(base_url=base, timeout=10) as c:
        r = c.post("/api/replays", json={"stream_id": 3, "target_seq": 2})
        replay_id = r.json()["id"]
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            hold = pool.submit(
                c.post,
                "/api/project",
                json={"stream_id": 3, "hold_seconds": 2.0},
            )
            _wait_for_lock(c, 3)
            blocked = c.post(f"/api/replays/{replay_id}/promote")
            assert hold.result().status_code == 200
        assert blocked.status_code == 409
        assert blocked.json()["detail"]["holder"]["operation"] == "run"


def test_failed_replay_is_reported_but_main_stays_intact(live_server):
    base = live_server
    with httpx.Client(base_url=base, timeout=10) as c:
        c.post("/api/project", json={"stream_id": 1})
        wm_before = c.get("/api/streams").json()[0]["watermark_seq"]

        r = c.post(
            "/api/replays",
            json={"stream_id": 1, "target_seq": 2, "force_fail": True},
        )
        assert r.status_code == 400
        assert "forced replay failure" in r.json()["detail"]

        assert c.get("/api/streams").json()[0]["watermark_seq"] == wm_before
        statuses = [x["status"] for x in c.get("/api/replays?stream_id=1").json()]
        assert "failed" in statuses


def test_invalid_target_seq_is_400_or_422_never_500(live_server):
    base = live_server
    with httpx.Client(base_url=base, timeout=10) as c:
        r = c.post("/api/replays", json={"stream_id": 1, "target_seq": 999})
        assert r.status_code == 400
        r = c.post("/api/replays", json={"stream_id": 1, "target_seq": -1})
        assert r.status_code == 422


def test_other_stream_not_blocked_by_lock(live_server):
    base = live_server
    with httpx.Client(base_url=base, timeout=10) as c:
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            hold = pool.submit(
                c.post,
                "/api/replays",
                json={"stream_id": 1, "target_seq": 2, "hold_seconds": 1.5},
            )
            _wait_for_lock(c, 1)
            other = c.post(
                "/api/replays", json={"stream_id": 2, "target_seq": 1}
            )
            assert hold.result().status_code == 200
        assert other.status_code == 200
        assert other.json()["status"] == "completed"

import os
import socket
import tempfile
import threading
import time

import pytest

# Point the data dir at an isolated temp location BEFORE app modules import
# config, so every test run uses its own sqlite file, never the real volume.
_TMP = tempfile.mkdtemp(prefix="changecast-test-")
os.environ.setdefault("DATA_DIR", _TMP)


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture()
def fresh_db():
    from app import config
    from app.seed import init_db

    for suffix in ("", "-wal", "-shm"):
        p = config.DATA_DIR / (config.DB_FILENAME + suffix)
        if p.exists():
            p.unlink()
    init_db()
    yield


@pytest.fixture()
def live_server(fresh_db):
    """A real uvicorn server in a daemon thread — required so concurrent
    requests exercise the actual per-stream DB lock across threads."""
    import uvicorn

    port = _free_port()
    config = uvicorn.Config(
        "app.main:app", host="127.0.0.1", port=port, log_level="warning"
    )
    server = uvicorn.Server(config)
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    deadline = time.time() + 10
    while time.time() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.5):
                break
        except OSError:
            time.sleep(0.1)
    else:
        raise RuntimeError("test server did not start")
    yield f"http://127.0.0.1:{port}"
    server.should_exit = True
    thread.join(timeout=5)

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from app.repositories.locks import StreamBusyError
from app.services import replay_service as svc

router = APIRouter(tags=["replays"])


class ReplayBody(BaseModel):
    stream_id: int
    target_seq: int = Field(ge=0)
    batch_size: int = Field(default=2, ge=1)
    hold_seconds: float = Field(default=0, ge=0, le=10)
    force_fail: bool = False


class PromoteBody(BaseModel):
    replay_id: int


def _translate(exc: Exception) -> HTTPException:
    if isinstance(exc, StreamBusyError):
        detail = str(exc)
        if exc.holder:
            detail = {
                "error": "stream_locked",
                "message": str(exc),
                "holder": exc.holder,
            }
        return HTTPException(409, detail)
    return HTTPException(400, str(exc))


@router.post("/replays")
def start_replay(body: ReplayBody):
    try:
        return svc.start_replay(
            body.stream_id,
            body.target_seq,
            batch_size=body.batch_size,
            hold_seconds=body.hold_seconds,
            force_fail=body.force_fail,
        )
    except (ValueError, StreamBusyError) as e:
        raise _translate(e) from e


@router.get("/replays")
def list_replays(stream_id: int | None = None, limit: int = 50):
    return svc.list_replays(stream_id=stream_id, limit=limit)


@router.get("/replays/{replay_id}")
def get_replay(replay_id: int):
    try:
        return svc.get_replay(replay_id)
    except ValueError as e:
        raise HTTPException(404, str(e)) from e


@router.get("/replays/{replay_id}/diff")
def replay_diff(replay_id: int):
    try:
        return svc.replay_diff(replay_id)
    except ValueError as e:
        raise HTTPException(400, str(e)) from e


@router.post("/replays/{replay_id}/promote")
def promote_replay(replay_id: int):
    try:
        return svc.promote_replay(replay_id)
    except (ValueError, StreamBusyError) as e:
        raise _translate(e) from e


@router.get("/stream-locks")
def list_locks():
    return svc.list_locks()

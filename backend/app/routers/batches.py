from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from app.repositories.locks import StreamBusyError
from app.services import project_service as svc

router = APIRouter(tags=["batches"])


class RunBody(BaseModel):
    stream_id: int
    note: str | None = None
    hold_seconds: float = Field(default=0, ge=0, le=10)


@router.get("/batches")
def list_batches(limit: int = 50):
    return svc.list_batches(limit=limit)


@router.post("/project")
def run_project(body: RunBody):
    try:
        return svc.run_project(
            body.stream_id, body.note, hold_seconds=body.hold_seconds
        )
    except StreamBusyError as e:
        raise HTTPException(
            409,
            detail={
                "error": "stream_locked",
                "message": str(e),
                "holder": e.holder,
            },
        ) from e
    except ValueError as e:
        raise HTTPException(400, str(e)) from e

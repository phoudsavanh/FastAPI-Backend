from fastapi import APIRouter
from app.models import LogsResponse
from app.memory import memory

router = APIRouter()

@router.get("/logs", response_model=LogsResponse, tags=["Device"])
async def get_logs(limit: int = 20):
    logs = memory.get_logs(limit)
    return {"logs": logs}
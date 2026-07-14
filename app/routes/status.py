from fastapi import APIRouter
from app.models import StatusResponse
from app.memory import memory

router = APIRouter()

@router.get("/status", response_model=StatusResponse, tags=["Device"])
async def get_status():
    return memory.get_system_status()
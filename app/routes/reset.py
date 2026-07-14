from fastapi import APIRouter
from app.memory import memory

router = APIRouter()

@router.post("/reset", tags=["Device"])
async def reset_all():
    await memory.reset_all()
    return {"status": "all relays OFF"}
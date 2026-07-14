from fastapi import APIRouter
from app.memory import memory

router = APIRouter()

@router.post("/reboot", tags=["Device"])
async def reboot_device():
    await memory.reboot()
    return {"status": "rebooting"}
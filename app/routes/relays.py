from fastapi import APIRouter, HTTPException
from app.models import RelayUpdate, RelaysResponse
from app.memory import memory

router = APIRouter()

@router.get("/relays", response_model=RelaysResponse, tags=["Relays"])
async def get_relays():
    names = memory.get_relay_names()
    states = memory.get_relays()
    return {"names": names, "states": states}

@router.put("/relays/{relay_id}", tags=["Relays"])
async def set_relay(relay_id: str, update: RelayUpdate):
    try:
        await memory.set_relay_async(relay_id, update.state)
    except ValueError:
        raise HTTPException(status_code=404, detail="Relay not found")
    return {"status": "ok", "relay": relay_id, "state": update.state}
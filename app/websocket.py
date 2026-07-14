import json
from fastapi import WebSocket, WebSocketDisconnect
from app.memory import memory


async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    memory.add_client(websocket)

    # Send current state immediately
    await websocket.send_json({"type": "state_update", "relays": memory.get_relays()})

    try:
        while True:
            raw = await websocket.receive_text()
            try:
                cmd = json.loads(raw)
                if cmd.get("command") == "set_relay":
                    relay_id = cmd.get("relay_id")
                    state = cmd.get("state")
                    if relay_id is not None and state is not None:
                        await memory.set_relay_async(relay_id, bool(state))
                elif cmd.get("command") == "reboot":
                    await memory.reboot()
                elif cmd.get("command") == "reset":
                    await memory.reset_all()
            except Exception:
                # ignore malformed commands
                pass
    except WebSocketDisconnect:
        memory.remove_client(websocket)
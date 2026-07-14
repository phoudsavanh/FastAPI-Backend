import time
import asyncio
from datetime import datetime
from typing import List, Dict
from sqlalchemy import select, update, func
from sqlalchemy.ext.asyncio import AsyncSession
from app.database import Relay, LogEntry, SystemState
from app.config import settings

RELAY_NAMES = {
    "D1": "Bedroom Main Light",
    "D2": "Bedroom Desk Lamp",
    "D3": "Living Room Ceiling",
    "D4": "Kitchen Overhead LED",
    "D5": "Bathroom Main Vent",
    "D6": "Balcony External Strip",
    "D7": "Garage Port Gate",
    "D8": "Garden Sprinkler Valve",
}

class DatabaseState:
    def __init__(self, db: AsyncSession):
        self.db = db

    async def _ensure_system_state(self):
        result = await self.db.execute(select(SystemState).limit(1))
        state = result.scalar_one_or_none()
        if not state:
            state = SystemState(
                ip=settings.NODE_IP,
                firmware=settings.FIRMWARE_VERSION,
                rssi=-45,
                status="Connected"
            )
            self.db.add(state)
            await self.db.commit()

    async def _ensure_relays(self):
        result = await self.db.execute(select(Relay.relay_id))
        existing_ids = {row[0] for row in result.all()}
        for relay_id, name in RELAY_NAMES.items():
            if relay_id not in existing_ids:
                self.db.add(Relay(relay_id=relay_id, name=name, state=False))
        await self.db.commit()

    async def initialize(self):
        await self._ensure_system_state()
        await self._ensure_relays()

    async def get_uptime(self) -> str:
        result = await self.db.execute(select(SystemState.boot_time).limit(1))
        boot = result.scalar_one_or_none()
        if not boot:
            return "0h 0m 0s"
        
        elapsed = int(time.time() - boot.timestamp())
        hours, rem = divmod(elapsed, 3600)
        minutes, seconds = divmod(rem, 60)
        return f"{hours}h {minutes}m {seconds}s"

    async def get_relays(self) -> Dict[str, bool]:
        result = await self.db.execute(select(Relay))
        relays = result.scalars().all()
        return {r.relay_id: r.state for r in relays}

    async def get_relay_names(self) -> Dict[str, str]:
        result = await self.db.execute(select(Relay))
        relays = result.scalars().all()
        return {r.relay_id: r.name for r in relays}

    async def set_relay(self, relay_id: str, state: bool) -> None:
        relay = await self.db.get(Relay, relay_id)
        if not relay:
            raise ValueError(f"Unknown relay: {relay_id}")
        relay.state = state
        log = LogEntry(message=f"Relay {relay_id} set to {'ON' if state else 'OFF'}")
        self.db.add(log)
        await self.db.commit()

    async def reboot(self) -> None:
        await self.db.execute(update(Relay).values(state=False))
        state = await self.db.get(SystemState, 1)
        if state:
            state.boot_time = func.now()
        log = LogEntry(message="Reboot triggered by API")
        self.db.add(log)
        await self.db.commit()

    async def reset_all(self) -> None:
        await self.db.execute(update(Relay).values(state=False))
        log = LogEntry(message="All relays reset to OFF")
        self.db.add(log)
        await self.db.commit()

    async def get_logs(self, limit: int = 20) -> List[str]:
        stmt = select(LogEntry).order_by(LogEntry.timestamp.desc()).limit(limit)
        result = await self.db.execute(stmt)
        logs = result.scalars().all()
        return [log.message for log in reversed(logs)]

    async def get_system_status(self) -> dict:
        result = await self.db.execute(select(SystemState).limit(1))
        state = result.scalar_one_or_none()
        return {
            "ip": state.ip if state else settings.NODE_IP,
            "firmware": state.firmware if state else settings.FIRMWARE_VERSION,
            "uptime": await self.get_uptime(),
            "rssi": state.rssi if state else -45,
            "status": state.status if state else "Connected",
        }
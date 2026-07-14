import asyncio
from datetime import datetime
from typing import Dict, List, Set
from sqlalchemy import select, update
from app.database import Relay, LogEntry, SystemState, AsyncSessionLocal
from app.config import settings


class MemoryState:
    _instance = None
    _lock = asyncio.Lock()

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance.relays: Dict[str, bool] = {}
            cls._instance.names: Dict[str, str] = {}
            cls._instance.system_status: Dict = {}
            cls._instance.logs: List[str] = []
            cls._instance.clients: Set = set()   # WebSocket connections
        return cls._instance

    async def load_from_db(self, db_session):
        """Load initial state from PostgreSQL on startup."""
        # Relays
        result = await db_session.execute(select(Relay))
        relays = result.scalars().all()
        self.relays = {r.relay_id: r.state for r in relays}
        self.names = {r.relay_id: r.name for r in relays}

        # System state
        result = await db_session.execute(select(SystemState).limit(1))
        state = result.scalar_one_or_none()
        if state:
            self.system_status = {
                "ip": state.ip,
                "firmware": state.firmware,
                "boot_time": state.boot_time,
                "rssi": state.rssi,
                "status": state.status,
            }
        else:
            self.system_status = {
                "ip": settings.NODE_IP,
                "firmware": settings.FIRMWARE_VERSION,
                "boot_time": datetime.now(),
                "rssi": -45,
                "status": "Connected",
            }

        # Recent logs
        stmt = select(LogEntry).order_by(LogEntry.timestamp.desc()).limit(20)
        result = await db_session.execute(stmt)
        logs = result.scalars().all()
        self.logs = [log.message for log in reversed(logs)]

    async def set_relay_async(self, relay_id: str, state: bool):
        """Update a relay instantly, broadcast change, persist asynchronously."""
        if relay_id not in self.relays:
            raise ValueError("Unknown relay")
        async with self._lock:
            self.relays[relay_id] = state
            self.logs.append(f"Relay {relay_id} set to {'ON' if state else 'OFF'}")
            if len(self.logs) > 100:
                self.logs.pop(0)
            await self._broadcast_state()
        # Fire‑and‑forget DB update
        asyncio.create_task(self._persist_relay(relay_id, state))

    async def reboot(self):
        """Turn all relays OFF, reset boot_time, broadcast, persist."""
        async with self._lock:
            for rid in self.relays:
                self.relays[rid] = False
            self.system_status["boot_time"] = datetime.now()
            self.logs.append("Reboot triggered by API")
            await self._broadcast_state()
        asyncio.create_task(self._persist_reboot())

    async def reset_all(self):
        """Turn all relays OFF, broadcast, persist."""
        async with self._lock:
            for rid in self.relays:
                self.relays[rid] = False
            self.logs.append("All relays reset to OFF")
            await self._broadcast_state()
        asyncio.create_task(self._persist_reset())

    # ---------- Read methods (no locks needed) ----------
    def get_relay_names(self) -> Dict[str, str]:
        return self.names

    def get_relays(self) -> Dict[str, bool]:
        return self.relays

    def get_system_status(self) -> dict:
        boot = self.system_status.get("boot_time")
        if boot:
            elapsed = int((datetime.now() - boot).total_seconds())
            h, rem = divmod(elapsed, 3600)
            m, s = divmod(rem, 60)
            uptime = f"{h}h {m}m {s}s"
        else:
            uptime = "0h 0m 0s"
        return {
            "ip": self.system_status.get("ip", settings.NODE_IP),
            "firmware": self.system_status.get("firmware", settings.FIRMWARE_VERSION),
            "uptime": uptime,
            "rssi": self.system_status.get("rssi", -45),
            "status": self.system_status.get("status", "Connected"),
        }

    def get_logs(self, limit: int = 20) -> List[str]:
        return self.logs[-limit:]

    # ---------- WebSocket client management ----------
    def add_client(self, ws):
        self.clients.add(ws)

    def remove_client(self, ws):
        self.clients.discard(ws)

    async def _broadcast_state(self):
        """Send current relay states to all connected WebSocket clients."""
        data = {"type": "state_update", "relays": self.relays}
        for client in list(self.clients):
            try:
                await client.send_json(data)
            except Exception:
                self.clients.remove(client)

    # ---------- Background persistence tasks ----------
    async def _persist_relay(self, relay_id: str, state: bool):
        async with AsyncSessionLocal() as db:
            relay = await db.get(Relay, relay_id)
            if relay:
                relay.state = state
                db.add(LogEntry(message=f"Relay {relay_id} set to {'ON' if state else 'OFF'}"))
                await db.commit()

    async def _persist_reboot(self):
        async with AsyncSessionLocal() as db:
            await db.execute(update(Relay).values(state=False))
            state = await db.get(SystemState, 1)
            if state:
                state.boot_time = datetime.now()
            db.add(LogEntry(message="Reboot triggered by API"))
            await db.commit()

    async def _persist_reset(self):
        async with AsyncSessionLocal() as db:
            await db.execute(update(Relay).values(state=False))
            db.add(LogEntry(message="All relays reset to OFF"))
            await db.commit()


# Global instance
memory = MemoryState()
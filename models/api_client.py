import time
import asyncio
import json
import requests
import threading
from typing import Dict, List, Callable, Optional
import concurrent.futures
import websockets
from queue import Queue

# --------------------- Base HTTP Client (common) ---------------------
class RelayAPIClient:
    def __init__(self, base_url: str):
        self.base_url = base_url.rstrip('/')
        self.ip = "unknown"
        self.status = "Offline"
        self.rssi = 0
        self.firmware = "unknown"
        self.uptime = "0h 0m 0s"

        self.default_names = {
            "D1": "Bedroom Main Light", "D2": "Bedroom Desk Lamp",
            "D3": "Living Room Ceiling", "D4": "Kitchen Overhead LED",
            "D5": "Bathroom Main Vent", "D6": "Balcony External Strip",
            "D7": "Garage Port Gate", "D8": "Garden Sprinkler Valve",
        }

        self.relay_names = self.default_names.copy()
        self.relay_states = {key: False for key in self.default_names}
        self.logs = ["Waiting for backend connection..."]
        self._refresh_all()

    def _refresh_all(self):
        try:
            # Status
            resp = requests.get(f"{self.base_url}/status", timeout=4)
            resp.raise_for_status()
            data = resp.json()
            self.ip = data.get("ip", self.ip)
            self.rssi = data.get("rssi", self.rssi)
            self.firmware = data.get("firmware", self.firmware)
            self.uptime = data.get("uptime", self.uptime)
            self.status = data.get("status", "Connected")

            # Relays
            resp = requests.get(f"{self.base_url}/relays", timeout=4)
            resp.raise_for_status()
            relay_data = resp.json()
            self.relay_names = relay_data.get("names", self.default_names)
            self.relay_states = relay_data.get("states", self.relay_states)

            # Logs
            resp = requests.get(f"{self.base_url}/logs?limit=20", timeout=4)
            resp.raise_for_status()
            log_data = resp.json()
            self.logs = log_data.get("logs", self.logs)

        except Exception as e:
            self.status = "Offline"
            self.logs.insert(0, f"Backend unreachable: {str(e)}")

    def set_relay(self, relay_id: str, state: bool) -> bool:
        try:
            resp = requests.put(
                f"{self.base_url}/relays/{relay_id}",
                json={"state": state},
                timeout=3
            )
            resp.raise_for_status()
            self.relay_states[relay_id] = state
            self.logs.insert(0, f"[{time.strftime('%H:%M:%S')}] Relay {relay_id} → {'ON' if state else 'OFF'}")
            return True
        except Exception as e:
            self.logs.insert(0, f"[ERROR] {relay_id}: {str(e)}")
            return False

    def set_all_relays(self, state: bool) -> Dict[str, bool]:
        results = {}
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
            futures = {executor.submit(self.set_relay, rid, state): rid for rid in self.relay_states}
            for future in concurrent.futures.as_completed(futures):
                rid = futures[future]
                results[rid] = future.result()
        return results

    def trigger_reboot(self):
        try:
            requests.post(f"{self.base_url}/reboot", timeout=3)
            self.logs.insert(0, f"[{time.strftime('%H:%M:%S')}] Reboot triggered")
            time.sleep(1)
            self._refresh_all()
        except Exception as e:
            self.logs.insert(0, f"[ERROR] Reboot failed: {str(e)}")

    def reset_all(self):
        try:
            requests.post(f"{self.base_url}/reset", timeout=3)
            for k in self.relay_states:
                self.relay_states[k] = False
            self.logs.insert(0, f"[{time.strftime('%H:%M:%S')}] All relays OFF")
        except Exception as e:
            self.logs.insert(0, f"[ERROR] Reset failed: {str(e)}")


# --------------------- Real‑Time Client (Thread-based WebSocket) ---------------------
class RealTimeRelayClient(RelayAPIClient):
    def __init__(self, base_url: str, on_state_update: Callable[[Dict[str, bool]], None]):
        super().__init__(base_url)
        self.on_state_update = on_state_update
        self.websocket_url = base_url.replace("http", "ws") + "/ws"
        self._running = False
        self._ws_thread: Optional[threading.Thread] = None
        self._event_loop: Optional[asyncio.AbstractEventLoop] = None
        self._message_queue = Queue()

    def start_websocket(self):
        """Start WebSocket in a dedicated background thread."""
        if self._running or self._ws_thread is not None:
            return
        
        self._running = True
        self._ws_thread = threading.Thread(daemon=True, target=self._run_websocket_thread)
        self._ws_thread.start()

    def stop_websocket(self):
        """Stop WebSocket gracefully."""
        self._running = False
        if self._ws_thread:
            self._ws_thread.join(timeout=2)
            self._ws_thread = None

    def _run_websocket_thread(self):
        """Run asyncio event loop in a separate thread."""
        try:
            self._event_loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self._event_loop)
            self._event_loop.run_until_complete(self._websocket_loop())
        except Exception as e:
            print(f"WebSocket thread error: {e}")
            self.logs.insert(0, f"WS thread fatal: {str(e)}")
        finally:
            if self._event_loop:
                self._event_loop.close()
            self._event_loop = None

    async def _websocket_loop(self):
        """Main WebSocket connection loop with reconnection logic."""
        retry_delay = 1
        while self._running:
            try:
                async with websockets.connect(
                    self.websocket_url,
                    ping_interval=20,
                    ping_timeout=10,
                    close_timeout=10
                ) as ws:
                    self.status = "Connected"
                    retry_delay = 1
                    self.logs.insert(0, f"[{time.strftime('%H:%M:%S')}] WebSocket connected")
                    
                    async for message in ws:
                        if not self._running:
                            break
                        try:
                            data = json.loads(message)
                            if data.get("type") == "state_update":
                                new_states = data.get("relays", {})
                                # Update local state (thread-safe)
                                self.relay_states.update(new_states)
                                # Queue callback for UI thread
                                if self.on_state_update:
                                    self.on_state_update(new_states)
                        except json.JSONDecodeError as e:
                            print(f"Invalid JSON: {e}")
                        except Exception as e:
                            print(f"WebSocket message error: {e}")
                            
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.status = "Offline"
                print(f"WebSocket connection failed: {e}")
                self.logs.insert(0, f"[{time.strftime('%H:%M:%S')}] WS disconnected: {str(e)[:50]}")
                
                if not self._running:
                    break
                    
                # Exponential backoff retry
                await asyncio.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, 30)


# --------------------- Mock Client (for testing without backend) ---------------------
class MockRelayClient:
    def __init__(self, ip: str = "192.168.10.103"):
        self.ip = ip
        self.status = "Connected"
        self.rssi = -45
        self.firmware = "v2.4.1-stable"
        self.uptime = "2d 4h 12m"
        self.relay_names = {
            "D1": "Bedroom Main Light", "D2": "Bedroom Desk Lamp",
            "D3": "Living Room Ceiling", "D4": "Kitchen Overhead LED",
            "D5": "Bathroom Main Vent", "D6": "Balcony External Strip",
            "D7": "Garage Port Gate", "D8": "Garden Sprinkler Valve"
        }
        self.relay_states = {f"D{i}": False for i in range(1, 9)}
        self.logs = [f"[{time.strftime('%H:%M:%S')}] Mock system initialized."]
        self._running = False

    def set_relay(self, key: str, state: bool) -> bool:
        if key not in self.relay_states:
            return False
        self.relay_states[key] = state
        self.logs.insert(0, f"[{time.strftime('%H:%M:%S')}] {self.relay_names[key]} → {'ON' if state else 'OFF'}")
        return True

    def set_all_relays(self, state: bool) -> Dict[str, bool]:
        for key in self.relay_states:
            self.set_relay(key, state)
        return {key: True for key in self.relay_states}

    def trigger_reboot(self):
        self.logs.insert(0, f"[{time.strftime('%H:%M:%S')}] Mock reboot completed")
        for k in self.relay_states:
            self.relay_states[k] = False

    def reset_all(self):
        for k in self.relay_states:
            self.relay_states[k] = False
        self.logs.insert(0, f"[{time.strftime('%H:%M:%S')}] All relays reset")

    def start_websocket(self):
        """Mock WebSocket (no-op)"""
        pass

    def stop_websocket(self):
        """Mock WebSocket (no-op)"""
        pass

from pydantic import BaseModel
from typing import Dict, List

class RelayUpdate(BaseModel):
    state: bool

class StatusResponse(BaseModel):
    ip: str
    firmware: str
    uptime: str
    rssi: int
    status: str

class RelaysResponse(BaseModel):
    names: Dict[str, str]
    states: Dict[str, bool]

class LogsResponse(BaseModel):
    logs: List[str]
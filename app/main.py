from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.config import settings
from app.database import init_db, AsyncSessionLocal
from app.memory import memory
from app.websocket import websocket_endpoint
from app.routes import (
    status_router,
    relays_router,
    reboot_router,
    logs_router,
    reset_router,
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: create tables and load memory from DB
    await init_db()
    async with AsyncSessionLocal() as db:
        await memory.load_from_db(db)
    yield
    # Shutdown: nothing special


app = FastAPI(title="NodeMCU Automation Core", version="1.0", lifespan=lifespan)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(status_router)
app.include_router(relays_router)
app.include_router(reboot_router)
app.include_router(logs_router)
app.include_router(reset_router)

# WebSocket endpoint for real‑time updates
app.add_api_websocket_route("/ws", websocket_endpoint)   # <-- fixed


@app.get("/", tags=["Root"])
async def root():
    return {"message": "NodeMCU Automation Core API", "docs": "/docs"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=True)
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import declarative_base
from sqlalchemy import Column, String, Boolean, Integer, DateTime, Text, func
from app.config import settings

engine = create_async_engine(
    settings.DATABASE_URL,
    echo=settings.DB_ECHO,
    future=True,
    pool_size=10,
    max_overflow=20,
    pool_pre_ping=True,
)

AsyncSessionLocal = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
Base = declarative_base()

# Models
class Relay(Base):
    __tablename__ = "relays"
    relay_id = Column(String, primary_key=True)
    name = Column(String, nullable=False)
    state = Column(Boolean, default=False)
    last_updated = Column(DateTime, server_default=func.now(), onupdate=func.now())

class LogEntry(Base):
    __tablename__ = "logs"
    id = Column(Integer, primary_key=True)
    timestamp = Column(DateTime, server_default=func.now())
    message = Column(Text, nullable=False)

class SystemState(Base):
    __tablename__ = "system_state"
    id = Column(Integer, primary_key=True)
    ip = Column(String)
    firmware = Column(String)
    boot_time = Column(DateTime, server_default=func.now())
    rssi = Column(Integer)
    status = Column(String)

async def init_db():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
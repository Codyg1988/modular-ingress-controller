import os
from typing import AsyncGenerator
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from sqlalchemy.orm import DeclarativeBase

DATABASE_URL = os.getenv("DATABASE_URL", "sqlite+aiosqlite:///./production_system.db")

engine = create_async_engine(
    DATABASE_URL,
    connect_args={"check_same_thread": False} if "sqlite" in DATABASE_URL else {},
    echo=False,
    future=True
)

AsyncSessionLocal = async_sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False,
    autocommit=False,
    autoflush=False
)

class Base(DeclarativeBase):
    pass

async def get_db() -> AsyncGenerator[AsyncSession, None]:
    """Dependency injection wrapper for fail-safe request-scoped database sessions."""
    async with AsyncSessionLocal() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()

async def initialize_database() -> None:
    """Creates all system schemas and tables if they do not exist on application startup."""
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

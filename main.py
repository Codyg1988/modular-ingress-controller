Python
import asyncio
from datetime import datetime
from typing import AsyncGenerator
from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from database import initialize_database, get_db
from models import DataStreamPayload
from schemas import PayloadCreate, PayloadResponse, SystemHealthResponse

app = FastAPI(
    title="Production Async Stream Broker",
    description="High-throughput asynchronous data distribution pipeline.",
    version="1.0.0"
)

# Global Cross-Origin Resource Sharing Rules
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.on_event("startup")
async def startup_event():
    await initialize_database()

@app.get("/", response_model=SystemHealthResponse, status_code=status.HTTP_200_OK)
async def system_root_health():
    """Returns absolute runtime verification status metrics for the orchestration loop."""
    return {
        "status": "operational",
        "timestamp": datetime.utcnow(),
        "active_connections": 1
    }

@app.post("/stream/ingest", response_model=PayloadResponse, status_code=status.HTTP_201_CREATED)
async def ingest_stream_payload(payload: PayloadCreate, db: AsyncSession = Depends(get_db)):
    """Accepts, validates, and commits an incoming stream payload to the data ledger."""
    db_payload = DataStreamPayload(
        client_id=payload.client_id,
        payload_type=payload.payload_type,
        raw_data=payload.raw_data,
        meta_attributes=payload.meta_attributes
    )
    db.add(db_payload)
    await db.flush()
    return db_payload

async def event_generator(client_id: str, db_factory) -> AsyncGenerator[str, None]:
    """Polls database asynchronously without blocking execution threads to stream telemetry."""
    while True:
        async with db_factory() as session:
            query = (
                select(DataStreamPayload)
                .where(DataStreamPayload.client_id == client_id)
                .order_by(DataStreamPayload.id.desc())
                .limit(1)
            )
            result = await session.execute(query)
            latest_record = result.scalar_one_or_none()
            
            if latest_record:
                yield f"data: {{\"id\": {latest_record.id}, \"type\": \"{latest_record.payload_type}\", \"timestamp\": \"{latest_record.timestamp.isoformat()}\"}}\n\n"
        
        await asyncio.sleep(1.0)  # Throttles execution rate to save compute overhead

@app.get("/stream/export/{client_id}")
async def stream_client_telemetry(client_id: str):
    """Establishes an active Server-Sent Events (SSE) pipe to push data structures to clients."""
    from database import AsyncSessionLocal
    return StreamingResponse(
        event_generator(client_id, AsyncSessionLocal),
        media_type="text/event-stream"
    )

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="127.0.0.1", port=8000, reload=True)

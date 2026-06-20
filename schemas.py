from datetime import datetime
from typing import Optional, Dict, Any
from pydantic import BaseModel, Field

class PayloadBase(BaseModel):
    client_id: str = Field(..., max_length=255, description="Unique identifier of the target client")
    payload_type: str = Field(..., max_length=100, description="Category of streaming event data")
    raw_data: Optional[str] = Field(None, description="Serialized data load payload")
    meta_attributes: Dict[str, Any] = Field(default_factory=dict, description="Structured key-value metadata mapping")

class PayloadCreate(PayloadBase):
    pass

class PayloadResponse(PayloadBase):
    id: int
    timestamp: datetime

    class Config:
        from_attributes = True

class SystemHealthResponse(BaseModel):
    status: str
    timestamp: datetime
    active_connections: int

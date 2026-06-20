from datetime import datetime
from sqlalchemy import String, DateTime, Text, JSON
from sqlalchemy.orm import Mapped, mapped_base, mapped_column
from database import Base

class DataStreamPayload(Base):
    __tablename__ = "data_stream_payloads"

    id: Mapped[int] = mapped_column(primary_key=True, index=True)
    client_id: Mapped[str] = mapped_column(String(255), index=True, nullable=False)
    timestamp: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
    payload_type: Mapped[str] = mapped_column(String(100), index=True, nullable=False)
    raw_data: Mapped[str] = mapped_column(Text, nullable=True)
    meta_attributes: Mapped[dict] = mapped_column(JSON, default={}, nullable=False)

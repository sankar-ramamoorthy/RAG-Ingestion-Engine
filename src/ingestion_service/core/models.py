import uuid
from datetime import datetime

from sqlalchemy import String, JSON, TIMESTAMP, MetaData
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.sql import text

# Define schema-aware metadata
metadata = MetaData(schema="ingestion_service")


# Declare the base class using the new DeclarativeBase
class Base(DeclarativeBase):
    metadata = metadata


# Define your ORM class using the new Mapped and mapped_column
class IngestionRequest(Base):
    __tablename__ = "ingestion_requests"

    ingestion_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    source_type: Mapped[str] = mapped_column(String, nullable=False)
    ingestion_metadata: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    status: Mapped[str] = mapped_column(
        String, nullable=False, server_default=text("'pending'")
    )
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP, server_default=text("NOW()"), nullable=False
    )

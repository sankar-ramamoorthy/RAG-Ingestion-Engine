from typing import Any, Dict, Literal, Optional
from uuid import UUID

from pydantic import BaseModel, Field


class IngestRequest(BaseModel):
    """
    Contract-only request model for ingestion.

    This model defines WHAT is being ingested,
    not HOW ingestion is performed.
    """

    source_type: Literal["file", "bytes", "uri"] = Field(
        ...,
        description="Type of source being ingested",
        examples=["file"],
    )

    metadata: Dict[str, Any] = Field(
        default_factory=dict,
        description="User-provided metadata to associate with the ingestion",
    )


class IngestResponse(BaseModel):
    """
    Contract-only response model for ingestion acceptance.
    """

    ingestion_id: UUID = Field(
        ...,
        description="Server-generated identifier for this ingestion request",
    )

    status: Literal["accepted"] = Field(
        ...,
        description="Indicates the ingestion request was accepted for processing",
        examples=["accepted"],
    )


class ErrorResponse(BaseModel):
    """
    Standard error envelope for all ingestion service errors.
    """

    error_code: str = Field(
        ...,
        description="Stable, machine-readable error code",
        examples=["INVALID_REQUEST"],
    )

    message: str = Field(
        ...,
        description="Human-readable error message",
    )

    details: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Optional structured error details",
    )

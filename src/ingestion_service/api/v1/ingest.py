from uuid import UUID, uuid4
import json
from typing import Optional

from fastapi import (
    APIRouter,
    HTTPException,
    UploadFile,
    File,
    Form,
    status,
)

from ingestion_service.api.v1.models import (
    IngestRequest,
    IngestResponse,
)

router = APIRouter(tags=["ingestion"])

# ------------------------------------------------------------------
# TEMPORARY in-memory registry (MS2 only)
# ------------------------------------------------------------------
_INGESTION_REGISTRY: set[UUID] = set()


# ==============================================================
# JSON INGESTION (CANONICAL CONTRACT — MS2)
# ==============================================================
@router.post(
    "/ingest",
    response_model=IngestResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Submit content for ingestion",
)
def ingest_json(request: IngestRequest) -> IngestResponse:
    """
    JSON-based ingestion.

    This is the canonical MS2 contract and the ONLY version
    exposed in OpenAPI.
    """
    ingestion_id = uuid4()
    _INGESTION_REGISTRY.add(ingestion_id)

    return IngestResponse(
        ingestion_id=ingestion_id,
        status="accepted",
    )


# ==============================================================
# MULTIPART FILE INGESTION (MS2a — UI ONLY)
# ==============================================================
@router.post(
    "/ingest/file",
    response_model=IngestResponse,
    status_code=status.HTTP_202_ACCEPTED,
    include_in_schema=False,  # UI-only
)
def ingest_file(
    file: UploadFile = File(...),
    metadata: Optional[str] = Form(default=None),
) -> IngestResponse:
    """
    Multipart file ingestion.

    UI / MVP convenience endpoint.
    NOT part of public contract.
    """
    try:
        parsed_metadata = json.loads(metadata) if metadata else {}
    except json.JSONDecodeError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid metadata JSON",
        ) from exc

    _ = IngestRequest(
        source_type="file",
        metadata={
            **parsed_metadata,
            "filename": file.filename,
        },
    )

    ingestion_id = uuid4()
    _INGESTION_REGISTRY.add(ingestion_id)

    return IngestResponse(
        ingestion_id=ingestion_id,
        status="accepted",
    )


# ==============================================================
# STATUS ENDPOINT (UNCHANGED)
# ==============================================================
@router.get(
    "/ingest/{ingestion_id}",
    response_model=IngestResponse,
    status_code=status.HTTP_200_OK,
    summary="Get ingestion status",
)
def ingest_status(ingestion_id: UUID) -> IngestResponse:
    if ingestion_id not in _INGESTION_REGISTRY:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Ingestion ID not found",
        )

    return IngestResponse(
        ingestion_id=ingestion_id,
        status="accepted",
    )

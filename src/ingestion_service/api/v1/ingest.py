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

from ingestion_service.api.v1.models import IngestRequest, IngestResponse
from ingestion_service.core.database_session import get_sessionmaker
from ingestion_service.core.models import IngestionRequest
from ingestion_service.core.pipeline import IngestionPipeline
from ingestion_service.core.status_manager import StatusManager
from ingestion_service.core.embedders.mock import MockEmbedder
from ingestion_service.core.vectorstore.memory import MemoryVectorStore

router = APIRouter(tags=["ingestion"])
SessionLocal = get_sessionmaker()


# ==============================================================
# MS2 NO-OP VALIDATOR
# ==============================================================
class NoOpValidator:
    def validate(self, text: str) -> None:
        return None


def _build_pipeline() -> IngestionPipeline:
    """
    MS2 synchronous pipeline with mock components.
    """
    return IngestionPipeline(
        validator=NoOpValidator(),
        embedder=MockEmbedder(),
        vector_store=MemoryVectorStore(),
    )


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
    ingestion_id = uuid4()

    with SessionLocal() as session:
        manager = StatusManager(session)

        manager.create_request(
            ingestion_id=ingestion_id,
            source_type=request.source_type,
            metadata=request.metadata,
        )

        manager.mark_running(ingestion_id)

        pipeline = _build_pipeline()
        try:
            pipeline.run(
                text="placeholder ingestion content",
                ingestion_id=str(ingestion_id),
            )
            manager.mark_completed(ingestion_id)
        except Exception as exc:
            manager.mark_failed(ingestion_id, error=str(exc))
            raise HTTPException(
                status_code=500,
                detail="Ingestion pipeline failed",
            ) from exc

    return IngestResponse(
        ingestion_id=ingestion_id,
        status="accepted",
    )


# ==============================================================
# MULTIPART FILE INGESTION (UI ONLY — MS2a)
# ==============================================================
@router.post(
    "/ingest/file",
    response_model=IngestResponse,
    status_code=status.HTTP_202_ACCEPTED,
    include_in_schema=False,
)
def ingest_file(
    file: UploadFile = File(...),
    metadata: Optional[str] = Form(default=None),
) -> IngestResponse:
    try:
        parsed_metadata = json.loads(metadata) if metadata else {}
    except json.JSONDecodeError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid metadata JSON",
        ) from exc

    ingestion_id = uuid4()

    # Read file content safely
    try:
        text = file.file.read().decode("utf-8")
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Unable to read uploaded file as UTF-8 text",
        ) from exc

    with SessionLocal() as session:
        manager = StatusManager(session)

        manager.create_request(
            ingestion_id=ingestion_id,
            source_type="file",
            metadata={
                **parsed_metadata,
                "filename": file.filename,
            },
        )

        manager.mark_running(ingestion_id)

        pipeline = _build_pipeline()
        try:
            pipeline.run(
                text=text,
                ingestion_id=str(ingestion_id),
            )
            manager.mark_completed(ingestion_id)
        except Exception as exc:
            manager.mark_failed(ingestion_id, error=str(exc))
            raise HTTPException(
                status_code=500,
                detail="Ingestion pipeline failed",
            ) from exc

    return IngestResponse(
        ingestion_id=ingestion_id,
        status="accepted",
    )


# ==============================================================
# STATUS ENDPOINT (DB-BACKED)
# ==============================================================
@router.get(
    "/ingest/{ingestion_id}",
    response_model=IngestResponse,
    status_code=status.HTTP_200_OK,
    summary="Get ingestion status",
)
def ingest_status(ingestion_id: UUID) -> IngestResponse:
    with SessionLocal() as session:
        request = (
            session.query(IngestionRequest).filter_by(ingestion_id=ingestion_id).first()
        )

        if request is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Ingestion ID not found",
            )

        return IngestResponse(
            ingestion_id=request.ingestion_id,
            status=request.status,
        )

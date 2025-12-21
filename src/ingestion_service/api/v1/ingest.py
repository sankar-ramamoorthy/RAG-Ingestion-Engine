from uuid import uuid4

from fastapi import APIRouter, status

from ingestion_service.api.v1.models import IngestRequest, IngestResponse

router = APIRouter(tags=["ingestion"])


@router.post(
    "/ingest",
    response_model=IngestResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Submit content for ingestion",
)
def ingest(request: IngestRequest) -> IngestResponse:
    """
    Accept an ingestion request.

    This endpoint validates the request and returns an ingestion ID.
    Actual ingestion processing is asynchronous and out of scope.
    """
    return IngestResponse(
        ingestion_id=uuid4(),
        status="accepted",
    )

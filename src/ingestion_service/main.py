from fastapi import FastAPI

from ingestion_service.api.health import router as health_router
from ingestion_service.api.v1 import router as v1_router
from ingestion_service.api.errors import register_error_handlers

# Register handlers before routers
app = FastAPI(title="Agentic RAG Ingestion Service")

register_error_handlers(app)

app.include_router(health_router)
app.include_router(v1_router)


@app.get("/")
def root():
    return {"service": "agentic-rag-ingestion"}

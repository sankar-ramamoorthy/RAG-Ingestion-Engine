from sqlalchemy import create_engine, MetaData
from sqlalchemy.orm import sessionmaker
from ingestion_service.core.config import settings

engine = create_engine(settings.DATABASE_URL, echo=True)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Use schema namespace
metadata = MetaData(schema="ingestion_service")

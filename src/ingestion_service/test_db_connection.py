from sqlalchemy import create_engine

# Database URL for the Postgres container
DATABASE_URL = "postgresql://ingestion_user:ingestion_pass@postgres:5432/ingestion_db"

# Create engine and try to connect to the DB
engine = create_engine(DATABASE_URL)

try:
    with engine.connect() as connection:
        print("Successfully connected to the database!")
except Exception as e:
    print(f"Error connecting to the database: {e}")

import os
from typing import Generator
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import URL, create_engine
from sqlalchemy.orm import Session, sessionmaker

load_dotenv(Path(__file__).resolve().parents[2] / ".env")

DB_USER = os.getenv("ACCOUNT_DB_USER") or os.getenv("DB_USER")
DB_PASSWORD = os.getenv("ACCOUNT_DB_PASSWORD") or os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("ACCOUNT_DB_HOST") or os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("ACCOUNT_DB_PORT") or os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("ACCOUNT_DB_NAME") or os.getenv("DB_NAME")

# URL.create handles special characters in credentials without interpolation.
DATABASE_URL = URL.create(
    "postgresql+psycopg2", username=DB_USER, password=DB_PASSWORD,
    host=DB_HOST, port=int(DB_PORT), database=DB_NAME,
)

engine = create_engine(DATABASE_URL, future=True, pool_pre_ping=True, connect_args={"connect_timeout": 5})
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine, future=True)


def get_db() -> Generator[Session, None, None]:
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

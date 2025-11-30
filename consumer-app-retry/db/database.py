# db/database.py
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import os

DB_HOST = os.getenv("POSTGRES_HOST", "postgres")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_NAME = os.getenv("POSTGRES_DB", "weather_alert_db")
DB_USER = os.getenv("POSTGRES_USER", "weather_user")
DB_PASS = os.getenv("POSTGRES_PASSWORD", "weather_pass")

DATABASE_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# Connection pool 적용
engine = create_engine(
    DATABASE_URL,
    pool_size=10,
    max_overflow=20,
    pool_pre_ping=True,
    pool_recycle=1800,
)

SessionLocal = sessionmaker(bind=engine, autocommit=False, autoflush=False)

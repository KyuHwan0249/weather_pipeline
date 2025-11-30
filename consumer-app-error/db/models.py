# db/models.py

from sqlalchemy import Column, Integer, String, Boolean, Text, TIMESTAMP
from sqlalchemy.orm import declarative_base
from sqlalchemy.sql import func

Base = declarative_base()

class ErrorWeatherData(Base):
    __tablename__ = "error_weather_data"

    id = Column(Integer, primary_key=True, index=True)
    error_type = Column(String, nullable=True)
    error_message = Column(Text, nullable=True)
    raw_row = Column(Text, nullable=True)   # JSON 문자열로 저장
    location = Column(String, nullable=True)
    file_name = Column(String, nullable=True)

    slack_sent = Column(Boolean, default=False)
    slack_sent_at = Column(TIMESTAMP(timezone=True), nullable=True)

    retry_count = Column(Integer, default=0)
    processing_status = Column(String, default="NEW")

    created_at = Column(TIMESTAMP(timezone=True), server_default=func.now())

# db/models.py

from sqlalchemy import Column, Integer, String, Boolean, Text, TIMESTAMP, Float
from sqlalchemy.orm import declarative_base
from sqlalchemy.sql import func

Base = declarative_base()

class WeatherAlert(Base):
    __tablename__ = "weather_alerts"

    alert_id = Column(Integer, primary_key=True, index=True)

    location = Column(String, nullable=False)
    alert_type = Column(String, nullable=False)
    alert_reason = Column(Text, nullable=False)

    event_time = Column(TIMESTAMP(timezone=True), nullable=False)
    value = Column(Float, nullable=True)
    threshold = Column(Float, nullable=True)

    slack_sent = Column(Boolean, default=False)
    slack_sent_at = Column(TIMESTAMP(timezone=True), nullable=True)

    raw_json = Column(Text, nullable=False)

    retry_count = Column(Integer, default=0)
    created_at = Column(TIMESTAMP(timezone=True), server_default=func.now())

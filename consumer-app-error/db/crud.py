# db/crud.py

import json
from sqlalchemy.orm import Session
from .models import ErrorWeatherData

def insert_error_row(db: Session, error_data: dict):

    record = ErrorWeatherData(
        error_type=error_data.get("error_type"),
        error_message=error_data.get("error_reason"),
        raw_row=json.dumps(error_data.get("raw_row")),
        location=error_data.get("raw_row", {}).get("Location"),
        file_name=error_data.get("file_name"),
        retry_count=error_data.get("retry_count", 0),
        processing_status="NEW",
    )

    db.add(record)
    db.commit()

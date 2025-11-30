# db/crud.py

import json
from sqlalchemy.orm import Session
from .models import WeatherAlert

def save_alert(
    db: Session,
    location,
    alert_type,
    alert_reason,
    event_time,
    value,
    threshold,
    raw_row,
    slack_sent=False,
    retry_count=0
):
    record = WeatherAlert(
        location=location,
        alert_type=alert_type,
        alert_reason=alert_reason,
        event_time=event_time,
        value=value,
        threshold=threshold,
        raw_json=json.dumps(raw_row, default=str),
        slack_sent=slack_sent,
        retry_count=retry_count
    )

    db.add(record)
    db.commit()
    db.refresh(record)

    return record.alert_id


def update_alert_sent(db: Session, alert_id: int):
    record = db.query(WeatherAlert).filter(WeatherAlert.alert_id == alert_id).first()
    if not record:
        return False

    record.slack_sent = True
    record.slack_sent_at = func.now()

    db.commit()
    return True

from db.connection import get_db_connection
import json

def save_alert(location, alert_type, alert_reason, event_time,
               value, threshold, raw_row, slack_sent=False):

    conn = get_db_connection()
    cur = conn.cursor()

    query = """
        INSERT INTO weather_alerts (
            location, alert_type, alert_reason,
            event_time, value, threshold,
            slack_sent, raw_json
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING alert_id;
    """

    cur.execute(query, (
        location, alert_type, alert_reason,
        event_time, value, threshold,
        slack_sent, json.dumps(raw_row, default=str)
    ))

    alert_id = cur.fetchone()[0]

    conn.commit()
    cur.close()
    conn.close()

    return alert_id


def update_alert_sent(alert_id):
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute(
        "UPDATE weather_alerts SET slack_sent = TRUE WHERE alert_id = %s;",
        (alert_id,)
    )

    conn.commit()
    cur.close()
    conn.close()

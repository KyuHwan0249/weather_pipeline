import psycopg2
import json
import os

DB_HOST = os.getenv("POSTGRES_HOST", "postgres")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_NAME = os.getenv("POSTGRES_DB", "weatherdb")
DB_USER = os.getenv("POSTGRES_USER", "weather")
DB_PASS = os.getenv("POSTGRES_PASSWORD", "weather123")

def get_conn():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS
    )

def insert_error_row(error_data):
    conn = get_conn()
    cur = conn.cursor()

    sql = """
    INSERT INTO error_weather_data
        (error_type, error_message, raw_row, location, file_name,
         slack_sent, slack_sent_at, retry_count, processing_status)
    VALUES (%s, %s, %s, %s, %s, false, NULL, 0, 'NEW')
    """

    cur.execute(sql, (
        error_data.get("error_type"),
        error_data.get("error_reason"),
        json.dumps(error_data.get("raw_row")),
        error_data.get("raw_row", {}).get("Location"),
        error_data.get("file_name")
    ))

    conn.commit()
    cur.close()
    conn.close()

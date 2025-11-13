CREATE TABLE IF NOT EXISTS weather_alerts (
    alert_id        SERIAL PRIMARY KEY,

    location        VARCHAR(100),
    alert_type      VARCHAR(50),
    alert_reason    TEXT,
    event_time      TIMESTAMP,

    value           DOUBLE PRECISION,
    threshold       DOUBLE PRECISION,

    created_at      TIMESTAMP DEFAULT NOW(),
    slack_sent      BOOLEAN DEFAULT FALSE,

    raw_json        JSONB
);
CREATE INDEX IF NOT EXISTS idx_weather_alerts_event_time ON weather_alerts(event_time);
CREATE INDEX IF NOT EXISTS idx_weather_alerts_location ON weather_alerts(location);
CREATE INDEX IF NOT EXISTS idx_weather_alerts_alert_type ON weather_alerts(alert_type);
CREATE INDEX IF NOT EXISTS idx_weather_alerts_slack_sent ON weather_alerts(slack_sent);

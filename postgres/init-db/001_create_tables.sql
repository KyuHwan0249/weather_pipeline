CREATE TABLE IF NOT EXISTS weather_alerts (
    alert_id        SERIAL PRIMARY KEY,

    -- Alert Dimensions
    location        VARCHAR(100) NOT NULL,
    alert_type      VARCHAR(50) NOT NULL,

    -- Alert Reason
    alert_reason    TEXT,
    event_time      TIMESTAMP NOT NULL,    -- event_time 기반 쿨다운 핵심

    -- Threshold Info
    value           DOUBLE PRECISION,
    threshold       DOUBLE PRECISION,

    -- Processing Info
    created_at      TIMESTAMP DEFAULT NOW(),
    slack_sent      BOOLEAN DEFAULT FALSE,
    slack_sent_at   TIMESTAMP,             -- Slack 보내진 시각

    -- Raw Kafka Row
    raw_json        JSONB
);

CREATE INDEX IF NOT EXISTS idx_weather_alerts_event_time ON weather_alerts(event_time);
CREATE INDEX IF NOT EXISTS idx_weather_alerts_location ON weather_alerts(location);
CREATE INDEX IF NOT EXISTS idx_weather_alerts_alert_type ON weather_alerts(alert_type);
CREATE INDEX IF NOT EXISTS idx_weather_alerts_slack_sent ON weather_alerts(slack_sent);

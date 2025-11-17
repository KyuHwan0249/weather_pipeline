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
    retry_count     INT DEFAULT 0,
    -- Raw Kafka Row
    raw_json        JSONB
);

CREATE TABLE error_weather_data (
    id SERIAL PRIMARY KEY,
    error_type VARCHAR(50) NOT NULL,         -- 에러 정보 -- e.g. SCHEMA_ERROR, TYPE_ERROR, MISSING_FIELD
    error_message TEXT NOT NULL,             -- detailed message
    raw_row JSONB NOT NULL, -- 원본 row 전체를 JSON 형식으로 저장
    location VARCHAR(50), -- 원본 메타정보
    file_name VARCHAR(255),
    retry_count INT DEFAULT 0, -- 재처리 관련 (옵션)
    processing_status VARCHAR(20) DEFAULT 'NEW',
    slack_sent BOOLEAN DEFAULT false,
    slack_sent_at TIMESTAMP NULL,
    created_at TIMESTAMP DEFAULT NOW() -- 에러 발생 시간
);


CREATE INDEX IF NOT EXISTS idx_weather_alerts_event_time ON weather_alerts(event_time);
CREATE INDEX IF NOT EXISTS idx_weather_alerts_location ON weather_alerts(location);
CREATE INDEX IF NOT EXISTS idx_weather_alerts_alert_type ON weather_alerts(alert_type);
CREATE INDEX IF NOT EXISTS idx_weather_alerts_slack_sent ON weather_alerts(slack_sent);

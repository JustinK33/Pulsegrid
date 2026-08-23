CREATE TABLE IF NOT EXISTS event (
    id BIGSERIAL PRIMARY KEY,
    timestamp BIGINT NOT NULL,
    visitorid BIGINT NOT NULL,
    event TEXT NOT NULL,
    itemid BIGINT NOT NULL,
    transactionid BIGINT,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS failed_events (
    id BIGSERIAL PRIMARY KEY,
    raw_message TEXT NOT NULL,
    error TEXT NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_event_event ON event (event);
CREATE INDEX IF NOT EXISTS idx_event_visitorid ON event (visitorid);

-- Curated layer. Rebuilt from scratch on every run.
-- DDL is transactional in Postgres, so readers never observe a half-built table.

DROP TABLE IF EXISTS event_clean CASCADE;
CREATE TABLE event_clean AS
SELECT DISTINCT
    timestamp AS event_ts_ms,
    (to_timestamp(timestamp / 1000) AT TIME ZONE 'UTC') AS event_at,
    (to_timestamp(timestamp / 1000) AT TIME ZONE 'UTC')::date AS event_date,
    visitorid,
    event,
    itemid,
    transactionid
FROM event;
-- ponytail: DISTINCT over the whole raw table, because re-running the DAG republishes
-- the same CSV and the loader has no upsert key. Swap for an incremental load on a
-- natural key once `event` outgrows a full scan.

CREATE INDEX idx_event_clean_visitorid ON event_clean (visitorid);
CREATE INDEX idx_event_clean_itemid ON event_clean (itemid);
CREATE INDEX idx_event_clean_event_date ON event_clean (event_date);

DROP TABLE IF EXISTS event_daily_counts;
CREATE TABLE event_daily_counts AS
SELECT
    event_date,
    event,
    count(*) AS events,
    count(DISTINCT visitorid) AS unique_visitors,
    count(DISTINCT itemid) AS unique_items
FROM event_clean
GROUP BY event_date, event;

DROP TABLE IF EXISTS visitor_funnel;
CREATE TABLE visitor_funnel AS
SELECT
    visitorid,
    count(*) FILTER (WHERE event = 'view') AS views,
    count(*) FILTER (WHERE event = 'addtocart') AS add_to_carts,
    count(*) FILTER (WHERE event = 'transaction') AS transactions,
    min(event_at) AS first_seen_at,
    max(event_at) AS last_seen_at
FROM event_clean
GROUP BY visitorid;

DROP TABLE IF EXISTS item_popularity;
CREATE TABLE item_popularity AS
WITH item_counts AS (
    SELECT
        itemid,
        count(*) FILTER (WHERE event = 'view') AS views,
        count(*) FILTER (WHERE event = 'addtocart') AS add_to_carts,
        count(*) FILTER (WHERE event = 'transaction') AS transactions
    FROM event_clean
    GROUP BY itemid
)
SELECT
    itemid,
    views,
    add_to_carts,
    transactions,
    round(add_to_carts::numeric / NULLIF(views, 0), 4) AS view_to_cart_rate,
    round(transactions::numeric / NULLIF(add_to_carts, 0), 4) AS cart_to_transaction_rate
FROM item_counts;

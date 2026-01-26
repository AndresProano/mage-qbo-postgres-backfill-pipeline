CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.qb_customers_backfill (
    id VARCHAR(50) PRIMARY KEY,
    payload JSONB,
    ingested_at_utc TIMESTAMP,
    extract_window_start_utc TIMESTAMP,
    extract_window_end_utc TIMESTAMP,
    page_number INTEGER,
    page_size INTEGER,
    request_payload JSONB
);

CREATE TABLE IF NOT EXISTS raw.qb_invoices_backfill (
    id VARCHAR(50) PRIMARY KEY,
    payload JSONB,
    ingested_at_utc TIMESTAMP,
    extract_window_start_utc TIMESTAMP,
    extract_window_end_utc TIMESTAMP,
    page_number INTEGER,
    page_size INTEGER,
    request_payload JSONB
);

CREATE TABLE IF NOT EXISTS raw.qb_items_backfill (
    id VARCHAR(50) PRIMARY KEY,
    payload JSONB,
    ingested_at_utc TIMESTAMP,
    extract_window_start_utc TIMESTAMP,
    extract_window_end_utc TIMESTAMP,
    page_number INTEGER,
    page_size INTEGER,
    request_payload JSONB
);
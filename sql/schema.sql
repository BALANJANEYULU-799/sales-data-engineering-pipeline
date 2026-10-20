-- ============================================================
-- sql/schema.sql
-- Database schema for the Sales Data Engineering Pipeline.
--
-- Includes:
--   1. Main orders_clean fact table
--   2. Star-schema dimension tables (dim_product, dim_date, dim_region)
--   3. fact_sales (dimensional model fact table)
--   4. Pipeline metadata table
--   5. Indexes for query performance
-- ============================================================


-- ──────────────────────────────────────────────────────────────
-- FLAT MODEL: orders_clean (used by the ETL pipeline directly)
-- ──────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS orders_clean (
    order_id         INTEGER PRIMARY KEY,
    customer_id      TEXT,
    customer_name    TEXT,
    email            TEXT,
    region           TEXT,
    product          TEXT    NOT NULL,
    category         TEXT    NOT NULL,
    quantity         INTEGER NOT NULL CHECK (quantity > 0),
    unit_price       REAL    NOT NULL CHECK (unit_price > 0),
    discount         REAL    NOT NULL DEFAULT 0.0,
    gross_amount     REAL    NOT NULL,
    discount_amount  REAL    NOT NULL DEFAULT 0.0,
    net_amount       REAL    NOT NULL,
    status           TEXT    NOT NULL DEFAULT 'Pending',
    order_date       TEXT    NOT NULL,
    order_year       INTEGER,
    order_month      INTEGER,
    order_quarter    INTEGER,
    order_month_name TEXT,
    is_discounted    INTEGER NOT NULL DEFAULT 0
);

-- Indexes on common filter and join columns
-- Why? Indexes allow the DB engine to find rows without a full table scan.
CREATE INDEX IF NOT EXISTS idx_orders_status     ON orders_clean(status);
CREATE INDEX IF NOT EXISTS idx_orders_category   ON orders_clean(category);
CREATE INDEX IF NOT EXISTS idx_orders_region     ON orders_clean(region);
CREATE INDEX IF NOT EXISTS idx_orders_date       ON orders_clean(order_date);
CREATE INDEX IF NOT EXISTS idx_orders_year_month ON orders_clean(order_year, order_month);


-- ──────────────────────────────────────────────────────────────
-- STAR SCHEMA: Dimension tables
--
-- Grain of fact_sales:
--   One row = one line item of one sales order.
--   This means the same order_id will NOT repeat (order_id is the grain).
--
-- Why star schema?
--   Separating products, dates, and regions into dimension tables allows
--   analysts to filter and group by attributes (category, quarter, region)
--   without storing redundant text in every fact row.
-- ──────────────────────────────────────────────────────────────

-- Dimension: Product
CREATE TABLE IF NOT EXISTS dim_product (
    product_key  INTEGER PRIMARY KEY AUTOINCREMENT,
    product_name TEXT    NOT NULL UNIQUE,
    category     TEXT    NOT NULL
);

-- Dimension: Date
-- Pre-populated with calendar attributes for fast time-based analysis.
CREATE TABLE IF NOT EXISTS dim_date (
    date_key     TEXT    PRIMARY KEY,   -- YYYY-MM-DD
    year         INTEGER NOT NULL,
    month        INTEGER NOT NULL,
    quarter      INTEGER NOT NULL,
    month_name   TEXT    NOT NULL,
    day_of_week  TEXT,
    is_weekend   INTEGER DEFAULT 0      -- 1 if Saturday or Sunday
);

-- Dimension: Region
CREATE TABLE IF NOT EXISTS dim_region (
    region_key   INTEGER PRIMARY KEY AUTOINCREMENT,
    region_name  TEXT    NOT NULL UNIQUE
);

-- Fact table: Sales
-- Foreign keys reference the dimension tables.
-- SQLite does not enforce FK constraints by default; they document intent.
CREATE TABLE IF NOT EXISTS fact_sales (
    fact_id         INTEGER PRIMARY KEY AUTOINCREMENT,
    order_id        INTEGER NOT NULL UNIQUE,         -- business key
    product_key     INTEGER REFERENCES dim_product(product_key),
    date_key        TEXT    REFERENCES dim_date(date_key),
    region_key      INTEGER REFERENCES dim_region(region_key),
    customer_id     TEXT,
    customer_name   TEXT,
    quantity        INTEGER NOT NULL CHECK (quantity > 0),
    unit_price      REAL    NOT NULL,
    discount        REAL    NOT NULL DEFAULT 0.0,
    gross_amount    REAL    NOT NULL,
    discount_amount REAL    NOT NULL DEFAULT 0.0,
    net_amount      REAL    NOT NULL,
    status          TEXT    NOT NULL
);

-- Index on fact table for common joins
CREATE INDEX IF NOT EXISTS idx_fact_date    ON fact_sales(date_key);
CREATE INDEX IF NOT EXISTS idx_fact_product ON fact_sales(product_key);
CREATE INDEX IF NOT EXISTS idx_fact_region  ON fact_sales(region_key);
CREATE INDEX IF NOT EXISTS idx_fact_status  ON fact_sales(status);


-- ──────────────────────────────────────────────────────────────
-- Pre-aggregated summary tables (for fast BI dashboards)
-- ──────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS summary_monthly (
    order_year       INTEGER NOT NULL,
    order_month      INTEGER NOT NULL,
    order_month_name TEXT,
    total_orders     INTEGER,
    total_revenue    REAL,
    avg_order_value  REAL,
    total_discount   REAL,
    PRIMARY KEY (order_year, order_month)
);

CREATE TABLE IF NOT EXISTS summary_by_category (
    category       TEXT PRIMARY KEY,
    total_orders   INTEGER,
    total_revenue  REAL,
    avg_price      REAL,
    total_units    INTEGER
);

CREATE TABLE IF NOT EXISTS summary_by_region (
    region           TEXT PRIMARY KEY,
    total_orders     INTEGER,
    total_revenue    REAL,
    avg_order_value  REAL
);

CREATE TABLE IF NOT EXISTS summary_top_products (
    product        TEXT,
    category       TEXT,
    total_orders   INTEGER,
    total_revenue  REAL,
    total_units    INTEGER
);

CREATE TABLE IF NOT EXISTS summary_by_status (
    status         TEXT PRIMARY KEY,
    count          INTEGER,
    total_revenue  REAL
);

-- ──────────────────────────────────────────────────────────────
-- Pipeline metadata table (original addition)
-- Tracks every ETL run for auditing and debugging.
-- ──────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id        INTEGER PRIMARY KEY AUTOINCREMENT,
    run_at        TEXT    NOT NULL,
    mode          TEXT    NOT NULL,
    rows_input    INTEGER,
    rows_loaded   INTEGER,
    rows_rejected INTEGER,
    elapsed_sec   REAL,
    status        TEXT
);

-- ──────────────────────────────────────────────────────────────
-- Incremental loading tracking table (original addition)
-- Records which order_ids have already been loaded.
-- ──────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS loaded_order_ids (
    order_id    INTEGER PRIMARY KEY,
    loaded_at   TEXT    NOT NULL,
    batch_label TEXT
);

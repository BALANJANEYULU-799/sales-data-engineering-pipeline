"""
src/database.py
===============
Storage & Analytical Loading Stage: Manages relational persistence into SQLite
using SQLAlchemy. Supports deterministic loading strategies:

  replace  – recreates target relational tables for clean full-refresh runs
  append   – appends incoming records to existing tables
  upsert   – executes idempotent insertion by filtering out already-loaded order_ids

Also manages:
  * Kimball Star Schema population (fact_sales, dim_product, dim_date, dim_region)
  * Metadata lineage logging into pipeline_runs audit table
  * Schema bootstrapping from DDL definitions
"""

import logging
import os
import sqlite3
from datetime import datetime

import pandas as pd
from sqlalchemy import create_engine, text

logger = logging.getLogger(__name__)


# ------------------------------------------------------------------ #
# Schema SQL (star schema additions are in sql/schema.sql)
# ------------------------------------------------------------------ #
ORDERS_DDL = """
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

CREATE INDEX IF NOT EXISTS idx_orders_status   ON orders_clean(status);
CREATE INDEX IF NOT EXISTS idx_orders_category ON orders_clean(category);
CREATE INDEX IF NOT EXISTS idx_orders_region   ON orders_clean(region);
CREATE INDEX IF NOT EXISTS idx_orders_date     ON orders_clean(order_date);
CREATE INDEX IF NOT EXISTS idx_orders_year_month ON orders_clean(order_year, order_month);
"""

PIPELINE_RUNS_DDL = """
CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id       INTEGER PRIMARY KEY AUTOINCREMENT,
    run_at       TEXT    NOT NULL,
    mode         TEXT    NOT NULL,
    rows_input   INTEGER,
    rows_loaded  INTEGER,
    rows_rejected INTEGER,
    elapsed_sec  REAL,
    status       TEXT
);
"""

STAR_SCHEMA_DDL = """
CREATE TABLE IF NOT EXISTS dim_product (
    product_key  INTEGER PRIMARY KEY AUTOINCREMENT,
    product_name TEXT    NOT NULL UNIQUE,
    category     TEXT    NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_date (
    date_key     TEXT    PRIMARY KEY,
    year         INTEGER NOT NULL,
    month        INTEGER NOT NULL,
    quarter      INTEGER NOT NULL,
    month_name   TEXT    NOT NULL,
    day_of_week  TEXT,
    is_weekend   INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS dim_region (
    region_key   INTEGER PRIMARY KEY AUTOINCREMENT,
    region_name  TEXT    NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS fact_sales (
    fact_id         INTEGER PRIMARY KEY AUTOINCREMENT,
    order_id        INTEGER NOT NULL UNIQUE,
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

CREATE INDEX IF NOT EXISTS idx_fact_date    ON fact_sales(date_key);
CREATE INDEX IF NOT EXISTS idx_fact_product ON fact_sales(product_key);
CREATE INDEX IF NOT EXISTS idx_fact_region  ON fact_sales(region_key);
CREATE INDEX IF NOT EXISTS idx_fact_status  ON fact_sales(status);
"""


class DatabaseLoader:
    """Loads clean DataFrames into SQLite with configurable strategies."""

    def __init__(self, db_url: str):
        self.db_url = db_url
        self.engine = create_engine(db_url)
        self._init_schema()

    # ------------------------------------------------------------------ #
    # Schema initialisation
    # ------------------------------------------------------------------ #
    def _init_schema(self):
        """Create required tables if they do not exist."""
        with self.engine.begin() as conn:
            for stmt in ORDERS_DDL.split(";"):
                if stmt.strip():
                    conn.execute(text(stmt))
            for stmt in PIPELINE_RUNS_DDL.split(";"):
                if stmt.strip():
                    conn.execute(text(stmt))
            for stmt in STAR_SCHEMA_DDL.split(";"):
                if stmt.strip():
                    conn.execute(text(stmt))
        logger.info("[DB] Schema initialised at %s", self.db_url)

    # ------------------------------------------------------------------ #
    # Load strategies
    # ------------------------------------------------------------------ #
    def load_replace(self, df: pd.DataFrame, table: str = "orders_clean") -> int:
        """
        Clear and reload the table with clean data while preserving table schema and primary key.
        Synchronizes the loaded_order_ids tracking table so incremental mode stays in sync.
        """
        df_to_load = self._prepare(df)
        with self.engine.begin() as conn:
            conn.execute(text(f"DELETE FROM {table}"))
            try:
                conn.execute(text("DELETE FROM loaded_order_ids"))
            except Exception:
                pass
        df_to_load.to_sql(table, self.engine, if_exists="append", index=False)

        # Sync tracking table for incremental mode
        try:
            loaded_at = datetime.now().isoformat()
            tracking_df = pd.DataFrame({
                "order_id": df_to_load["order_id"].tolist(),
                "loaded_at": loaded_at,
                "batch_label": "full_replace",
            })
            tracking_df.to_sql("loaded_order_ids", self.engine, if_exists="append", index=False)
        except Exception as e:
            logger.debug("Tracking table sync skipped: %s", e)

        logger.info("[DB] REPLACE | table=%s | %d rows loaded", table, len(df_to_load))
        return len(df_to_load)

    def load_upsert(self, df: pd.DataFrame, table: str = "orders_clean") -> dict:
        """
        Idempotent upsert: insert rows whose order_id does not already exist.
        Returns counts: new, skipped.

        This is an original feature — not present in the reference project.
        """
        df_to_load = self._prepare(df)

        # Fetch existing order_ids
        with self.engine.connect() as conn:
            try:
                existing_ids = set(
                    pd.read_sql(text(f"SELECT order_id FROM {table}"), conn)["order_id"].tolist()
                )
            except Exception:
                existing_ids = set()

        new_rows = df_to_load[~df_to_load["order_id"].isin(existing_ids)]
        skipped = len(df_to_load) - len(new_rows)

        if not new_rows.empty:
            new_rows.to_sql(table, self.engine, if_exists="append", index=False)

        logger.info(
            "[DB] UPSERT | table=%s | new=%d | skipped(existing)=%d",
            table, len(new_rows), skipped,
        )
        return {"new": len(new_rows), "skipped": skipped}

    # ------------------------------------------------------------------ #
    # Record pipeline run metadata
    # ------------------------------------------------------------------ #
    def record_pipeline_run(self, meta: dict) -> None:
        """
        Insert one row into the pipeline_runs metadata table.
        This lets you track every execution's input/output counts.
        """
        row = pd.DataFrame([{
            "run_at": meta.get("run_at", datetime.now().isoformat()),
            "mode": meta.get("mode", "replace"),
            "rows_input": meta.get("rows_input", 0),
            "rows_loaded": meta.get("rows_loaded", 0),
            "rows_rejected": meta.get("rows_rejected", 0),
            "elapsed_sec": meta.get("elapsed_sec", 0.0),
            "status": meta.get("status", "COMPLETE"),
        }])
        row.to_sql("pipeline_runs", self.engine, if_exists="append", index=False)
        logger.info("[DB] Pipeline run recorded in metadata table.")

    # ------------------------------------------------------------------ #
    # Summary tables  (pre-aggregated for fast BI queries)
    # ------------------------------------------------------------------ #
    def build_summaries(self, df: pd.DataFrame) -> dict:
        """Build and load 5 pre-aggregated summary tables."""
        summaries = {}

        monthly = (
            df.groupby(["order_year", "order_month", "order_month_name"])
            .agg(
                total_orders=("order_id", "count"),
                total_revenue=("net_amount", "sum"),
                avg_order_value=("net_amount", "mean"),
                total_discount=("discount_amount", "sum"),
            )
            .reset_index()
            .round(2)
        )
        monthly.to_sql("summary_monthly", self.engine, if_exists="replace", index=False)
        summaries["monthly"] = monthly

        by_cat = (
            df.groupby("category")
            .agg(
                total_orders=("order_id", "count"),
                total_revenue=("net_amount", "sum"),
                avg_price=("unit_price", "mean"),
                total_units=("quantity", "sum"),
            )
            .reset_index()
            .sort_values("total_revenue", ascending=False)
            .round(2)
        )
        by_cat.to_sql("summary_by_category", self.engine, if_exists="replace", index=False)
        summaries["by_category"] = by_cat

        by_region = (
            df.groupby("region")
            .agg(
                total_orders=("order_id", "count"),
                total_revenue=("net_amount", "sum"),
                avg_order_value=("net_amount", "mean"),
            )
            .reset_index()
            .sort_values("total_revenue", ascending=False)
            .round(2)
        )
        by_region.to_sql("summary_by_region", self.engine, if_exists="replace", index=False)
        summaries["by_region"] = by_region

        top_products = (
            df.groupby(["product", "category"])
            .agg(
                total_orders=("order_id", "count"),
                total_revenue=("net_amount", "sum"),
                total_units=("quantity", "sum"),
            )
            .reset_index()
            .sort_values("total_revenue", ascending=False)
            .round(2)
        )
        top_products.to_sql("summary_top_products", self.engine, if_exists="replace", index=False)
        summaries["top_products"] = top_products

        by_status = (
            df.groupby("status")
            .agg(count=("order_id", "count"), total_revenue=("net_amount", "sum"))
            .reset_index()
            .round(2)
        )
        by_status.to_sql("summary_by_status", self.engine, if_exists="replace", index=False)
        summaries["by_status"] = by_status

        logger.info("[DB] Summary tables built: %s", list(summaries.keys()))
        return summaries

    # ------------------------------------------------------------------ #
    # Star schema dimension & fact loader
    # ------------------------------------------------------------------ #
    def build_star_schema(self, df: pd.DataFrame) -> dict:
        """
        Populate dim_product, dim_region, dim_date, and fact_sales tables
        from the clean orders DataFrame to provide a dimensional model.
        """
        df_clean = df.copy()
        if "order_date" in df_clean.columns and not pd.api.types.is_string_dtype(df_clean["order_date"]):
            dates_dt = pd.to_datetime(df_clean["order_date"], errors="coerce")
            df_clean["date_key"] = dates_dt.dt.strftime("%Y-%m-%d")
        else:
            df_clean["date_key"] = df_clean["order_date"].astype(str)
            dates_dt = pd.to_datetime(df_clean["date_key"], errors="coerce")

        with self.engine.begin() as conn:
            conn.execute(text("DELETE FROM fact_sales"))
            conn.execute(text("DELETE FROM dim_product"))
            conn.execute(text("DELETE FROM dim_region"))
            conn.execute(text("DELETE FROM dim_date"))

        # 1. dim_product
        prods = (
            df_clean[["product", "category"]]
            .drop_duplicates()
            .rename(columns={"product": "product_name"})
        )
        prods.to_sql("dim_product", self.engine, if_exists="append", index=False)

        # 2. dim_region
        regions = (
            df_clean[["region"]]
            .drop_duplicates()
            .rename(columns={"region": "region_name"})
        )
        regions.to_sql("dim_region", self.engine, if_exists="append", index=False)

        # 3. dim_date
        date_df = (
            pd.DataFrame({
                "date_key": df_clean["date_key"],
                "year": dates_dt.dt.year.fillna(0).astype(int),
                "month": dates_dt.dt.month.fillna(0).astype(int),
                "quarter": dates_dt.dt.quarter.fillna(0).astype(int),
                "month_name": dates_dt.dt.strftime("%B").fillna("Unknown"),
                "day_of_week": dates_dt.dt.strftime("%A").fillna("Unknown"),
                "is_weekend": dates_dt.dt.dayofweek.isin([5, 6]).astype(int),
            })
            .drop_duplicates(subset=["date_key"])
            .dropna(subset=["date_key"])
        )
        date_df.to_sql("dim_date", self.engine, if_exists="append", index=False)

        # 4. Map dimension keys to build fact_sales
        with self.engine.connect() as conn:
            dim_prod_map = pd.read_sql(
                text("SELECT product_key, product_name FROM dim_product"), conn
            )
            dim_reg_map = pd.read_sql(
                text("SELECT region_key, region_name FROM dim_region"), conn
            )

        fact = df_clean.merge(
            dim_prod_map, left_on="product", right_on="product_name", how="left"
        )
        fact = fact.merge(
            dim_reg_map, left_on="region", right_on="region_name", how="left"
        )

        fact_cols = [
            "order_id",
            "product_key",
            "date_key",
            "region_key",
            "customer_id",
            "customer_name",
            "quantity",
            "unit_price",
            "discount",
            "gross_amount",
            "discount_amount",
            "net_amount",
            "status",
        ]
        fact_to_load = fact[fact_cols].drop_duplicates(subset=["order_id"])
        fact_to_load.to_sql("fact_sales", self.engine, if_exists="append", index=False)

        logger.info(
            "[DB] Star schema populated: fact_sales (%d rows), dim_product (%d rows), dim_region (%d rows), dim_date (%d rows)",
            len(fact_to_load), len(prods), len(regions), len(date_df),
        )
        return {
            "fact_sales": len(fact_to_load),
            "dim_product": len(prods),
            "dim_region": len(regions),
            "dim_date": len(date_df),
        }

    # ------------------------------------------------------------------ #
    # Verify
    # ------------------------------------------------------------------ #
    def verify(self, table: str = "orders_clean") -> int:
        with self.engine.connect() as conn:
            count = conn.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar()
        logger.info("[DB] Verified '%s' | %d rows in DB", table, count)
        return count

    # ------------------------------------------------------------------ #
    # Private helpers
    # ------------------------------------------------------------------ #
    def _prepare(self, df: pd.DataFrame) -> pd.DataFrame:
        """Convert date columns to strings for SQLite storage."""
        df = df.copy()
        if "order_date" in df.columns and pd.api.types.is_datetime64_any_dtype(df["order_date"]):
            df["order_date"] = df["order_date"].dt.strftime("%Y-%m-%d")
        # SQLite does not support pandas Int64 NA-aware integers
        for col in ["order_year", "order_month", "order_quarter", "order_id", "quantity"]:
            if col in df.columns:
                df[col] = df[col].astype("float").astype("Int64").astype(object)
        return df

"""
src/database.py
===============
Database loading stage: loads validated records into SQLite using
SQLAlchemy.  Supports three modes:

  replace    – drop and recreate the table on each run (default for first run)
  append     – add rows to an existing table (use with caution for duplicates)
  upsert     – insert new rows; skip rows whose order_id already exists
               (idempotent / incremental mode)

This module is an original addition to the reference project.  The reference
project uses pandas to_sql with if_exists='replace' only, without
idempotency or upsert logic.

Attribution
-----------
Loader class skeleton inspired by reference project (Pravalika Kadakanchi /
ETL-Pipeline). The upsert / incremental logic, pipeline_runs metadata table,
and explicit schema-creation from SQL file are original additions.
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
        logger.info("[DB] Schema initialised at %s", self.db_url)

    # ------------------------------------------------------------------ #
    # Load strategies
    # ------------------------------------------------------------------ #
    def load_replace(self, df: pd.DataFrame, table: str = "orders_clean") -> int:
        """
        Drop and recreate the table with clean data.
        Use for a full reload.  Not idempotent.
        """
        df_to_load = self._prepare(df)
        df_to_load.to_sql(table, self.engine, if_exists="replace", index=False)
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

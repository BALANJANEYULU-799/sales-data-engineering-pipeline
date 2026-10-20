"""
src/incremental.py
==================
Incremental loading demonstration.

This module is an ORIGINAL ADDITION to the reference project.  The reference
project reloads the entire table on each run (if_exists='replace').

How incremental loading works here
------------------------------------
1. A 'loaded_order_ids' tracking table stores the order_ids that have
   already been successfully loaded into the database.
2. When a new batch arrives, we compare its order_ids against the tracking
   table and classify each record as:
     - NEW      → load it
     - EXISTING → skip (or update if business rule requires)
     - REJECTED → quarantined during transformation (not loaded)
3. The new records are inserted; the tracking table is updated.
4. Running the SAME batch again loads 0 new rows (idempotency check).

This approach is simple, beginner-friendly, and demonstrates the key idea
of incremental / change-data-capture pipelines without Airflow or Spark.
"""

import logging
from datetime import datetime

import pandas as pd
from sqlalchemy import create_engine, text

logger = logging.getLogger(__name__)


class IncrementalLoader:
    """
    Tracks processed records and supports idempotent batch loading.
    """

    TRACKING_TABLE = "loaded_order_ids"
    TRACKING_DDL = """
    CREATE TABLE IF NOT EXISTS loaded_order_ids (
        order_id    INTEGER PRIMARY KEY,
        loaded_at   TEXT NOT NULL,
        batch_label TEXT
    );
    """

    def __init__(self, db_url: str):
        self.db_url = db_url
        self.engine = create_engine(db_url)
        self._init_tracking_table()

    def _init_tracking_table(self):
        with self.engine.begin() as conn:
            conn.execute(text(self.TRACKING_DDL))
        logger.info("[INCR] Tracking table ready: %s", self.TRACKING_TABLE)

    # ------------------------------------------------------------------ #
    # Main incremental load method
    # ------------------------------------------------------------------ #
    def load_batch(
        self,
        clean_df: pd.DataFrame,
        main_table: str = "orders_clean",
        batch_label: str = "",
    ) -> dict:
        """
        Load a batch of clean records incrementally.

        Parameters
        ----------
        clean_df    : transformed DataFrame ready for loading
        main_table  : target table in SQLite
        batch_label : optional label (e.g., "batch_001") for auditing

        Returns
        -------
        dict with keys: new, skipped, batch_label, loaded_at
        """
        if "order_id" not in clean_df.columns:
            raise ValueError("DataFrame must have an 'order_id' column for incremental loading.")

        # Get already-processed order_ids
        with self.engine.connect() as conn:
            try:
                existing_ids = set(
                    pd.read_sql(
                        text(f"SELECT order_id FROM {self.TRACKING_TABLE}"),
                        conn,
                    )["order_id"].tolist()
                )
            except Exception:
                existing_ids = set()

        # Split batch into new vs already-loaded
        new_df = clean_df[~clean_df["order_id"].isin(existing_ids)].copy()
        skipped_count = len(clean_df) - len(new_df)

        loaded_at = datetime.now().isoformat()

        if not new_df.empty:
            # Prepare date column for SQLite
            if "order_date" in new_df.columns and pd.api.types.is_datetime64_any_dtype(new_df["order_date"]):
                new_df["order_date"] = new_df["order_date"].dt.strftime("%Y-%m-%d")

            # Convert pandas Int64 to object for SQLite compatibility
            for col in ["order_year", "order_month", "order_quarter", "order_id", "quantity"]:
                if col in new_df.columns:
                    new_df[col] = new_df[col].astype("float").astype("Int64").astype(object)

            new_df.to_sql(main_table, self.engine, if_exists="append", index=False)

            # Update tracking table
            tracking_rows = pd.DataFrame({
                "order_id": new_df["order_id"].tolist(),
                "loaded_at": loaded_at,
                "batch_label": batch_label or "default",
            })
            tracking_rows.to_sql(
                self.TRACKING_TABLE, self.engine,
                if_exists="append", index=False,
            )

        logger.info(
            "[INCR] Batch '%s' | new=%d | skipped(already loaded)=%d",
            batch_label, len(new_df), skipped_count,
        )

        return {
            "batch_label": batch_label,
            "loaded_at": loaded_at,
            "new": len(new_df),
            "skipped": skipped_count,
        }

    # ------------------------------------------------------------------ #
    # Status reporting
    # ------------------------------------------------------------------ #
    def get_loaded_count(self, main_table: str = "orders_clean") -> int:
        """Return the number of rows currently in the main table."""
        with self.engine.connect() as conn:
            try:
                return conn.execute(text(f"SELECT COUNT(*) FROM {main_table}")).scalar()
            except Exception:
                return 0

    def get_batch_history(self) -> pd.DataFrame:
        """Return a summary of all processed batches."""
        with self.engine.connect() as conn:
            try:
                return pd.read_sql(
                    text(
                        f"SELECT batch_label, COUNT(*) AS records_loaded, MIN(loaded_at) AS loaded_at "
                        f"FROM {self.TRACKING_TABLE} GROUP BY batch_label ORDER BY loaded_at"
                    ),
                    conn,
                )
            except Exception:
                return pd.DataFrame(columns=["batch_label", "records_loaded", "loaded_at"])

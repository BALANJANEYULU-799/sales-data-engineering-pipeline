"""
src/transformation.py
=====================
Transformation stage: cleaning, type casting, business-key deduplication,
derived-column computation, and quarantine of rejected records.

Attribution note
----------------
Core transformation steps (column normalisation, type casting, status/region
cleaning, numeric range validation, derived columns) are adapted from the
reference project by Pravalika Kadakanchi
(https://github.com/PravalikaKadakanchi/ETL-Pipeline, MIT-style licence).

Original additions in this project
-----------------------------------
* Business-key deduplication (by order_id) with explicit policy documentation
  — the reference project uses generic row-level exact deduplication.
* Quarantine CSV: rejected records are written to a separate file with a
  documented rejection reason column rather than being silently dropped.
* Cleaning summary dict returned alongside the clean DataFrame.
* customer_id and customer_name null filling with documented defaults.
"""

import logging
import re

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

EMAIL_RE = re.compile(r"^[\w.+-]+@[\w-]+\.[a-zA-Z]{2,}$")

VALID_STATUSES = {"Completed", "Pending", "Cancelled", "Refunded"}
VALID_REGIONS = {"North", "South", "East", "West", "Central"}

# Business key for deduplication — documented rule
BUSINESS_KEY = ["order_id"]


class Transformer:
    """
    Cleans and transforms raw orders data through a sequential pipeline.

    Each step appends an entry to self.audit so callers can track
    exactly what changed.  Rejected records are accumulated in
    self.quarantine for export to a quarantine CSV.
    """

    def __init__(self, config: dict):
        self.config = config
        self.audit: list[dict] = []
        self.quarantine: list[dict] = []   # records rejected with reasons

    # ------------------------------------------------------------------ #
    # Master pipeline
    # ------------------------------------------------------------------ #
    def run(self, df: pd.DataFrame) -> pd.DataFrame:
        original_rows = len(df)
        logger.info("[TRANSFORM] Starting pipeline | %d input rows", original_rows)

        df = self.normalise_column_names(df)
        df = self.cast_types(df)
        df = self.clean_status(df)
        df = self.clean_region(df)
        df = self.validate_email(df)
        df = self.handle_missing_values(df)
        df = self.validate_numeric_ranges(df)
        df = self.deduplicate_by_business_key(df)
        df = self.compute_derived_columns(df)
        df = self.final_sort(df)

        logger.info(
            "[TRANSFORM] Pipeline complete | %d clean rows (%d removed)",
            len(df), original_rows - len(df),
        )
        return df

    # ------------------------------------------------------------------ #
    # Step 1: Normalise column names
    # ------------------------------------------------------------------ #
    def normalise_column_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """Strip whitespace, lowercase, replace spaces with underscores."""
        df = df.copy()
        df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
        self._log("normalise_column_names", len(df), len(df), "Column names normalised")
        return df

    # ------------------------------------------------------------------ #
    # Step 2: Type casting
    # ------------------------------------------------------------------ #
    def cast_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Cast order_date to datetime (handles YYYY-MM-DD and DD/MM/YYYY).
        Cast quantity, unit_price, discount to numeric; invalid → NaN.
        """
        df = df.copy()
        before = len(df)

        # Try ISO format first, then day-first for mixed-format CSVs
        df["order_date"] = pd.to_datetime(df["order_date"], dayfirst=False, errors="coerce")
        # Re-attempt day-first for values that failed (still NaT)
        mask_nat = df["order_date"].isna()
        if mask_nat.any():
            retry = pd.to_datetime(
                df.loc[mask_nat, "order_date"], dayfirst=True, errors="coerce"
            )
            df.loc[mask_nat, "order_date"] = retry

        for col in ["quantity", "unit_price", "discount"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        self._log("cast_types", before, len(df), "Types cast: date (multi-format), numerics")
        return df

    # ------------------------------------------------------------------ #
    # Step 3: Normalise status field
    # ------------------------------------------------------------------ #
    def clean_status(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Title-case status values.  Values not in VALID_STATUSES → NaN.
        These will be filled with 'Pending' in handle_missing_values.
        """
        df = df.copy()
        df["status"] = df["status"].str.strip().str.title()
        invalid_mask = ~df["status"].isin(VALID_STATUSES) & df["status"].notna()
        df.loc[invalid_mask, "status"] = np.nan
        self._log(
            "clean_status", len(df), len(df),
            f"Status normalised; {invalid_mask.sum()} invalid values → NaN",
        )
        return df

    # ------------------------------------------------------------------ #
    # Step 4: Normalise region field
    # ------------------------------------------------------------------ #
    def clean_region(self, df: pd.DataFrame) -> pd.DataFrame:
        """Replace 'N/A' / '' with NaN; title-case; reject unknown regions."""
        df = df.copy()
        df["region"] = df["region"].replace({"N/A": np.nan, "": np.nan})
        df["region"] = df["region"].str.strip().str.title()
        invalid_mask = ~df["region"].isin(VALID_REGIONS) & df["region"].notna()
        df.loc[invalid_mask, "region"] = np.nan
        self._log("clean_region", len(df), len(df), "Region values normalised")
        return df

    # ------------------------------------------------------------------ #
    # Step 5: Email validation
    # ------------------------------------------------------------------ #
    def validate_email(self, df: pd.DataFrame) -> pd.DataFrame:
        """Regex-check emails; malformed values → NaN (not quarantined)."""
        if "email" not in df.columns:
            return df
        df = df.copy()
        invalid = df["email"].apply(
            lambda x: not bool(EMAIL_RE.match(str(x))) if pd.notna(x) else False
        )
        df.loc[invalid, "email"] = np.nan
        self._log(
            "validate_email", len(df), len(df),
            f"Invalidated {int(invalid.sum())} malformed emails",
        )
        return df

    # ------------------------------------------------------------------ #
    # Step 6: Handle missing values  — with quarantine
    # ------------------------------------------------------------------ #
    def handle_missing_values(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Rejection rule (documented):
          Rows missing ANY of [order_id, product, unit_price, order_date]
          are moved to the quarantine list with a reason string.

        Non-critical fields are filled with documented defaults:
          status   → 'Pending'
          discount → 0.0
          region   → 'Unknown'
          customer_name → 'Anonymous'
          customer_id   → 'UNKNOWN'
        """
        df = df.copy()
        before = len(df)

        critical = ["order_id", "product", "unit_price", "order_date"]
        present_critical = [c for c in critical if c in df.columns]

        # Build rejection mask
        reject_mask = df[present_critical].isnull().any(axis=1)
        rejected = df[reject_mask].copy()
        rejected["rejection_reason"] = rejected[present_critical].apply(
            lambda row: "Missing critical field(s): "
            + ", ".join(col for col in present_critical if pd.isna(row[col])),
            axis=1,
        )
        self.quarantine.extend(rejected.to_dict("records"))
        df = df[~reject_mask].copy()

        # Fill non-critical fields
        fill_rules = {
            "status": "Pending",
            "discount": 0.0,
            "region": "Unknown",
            "customer_name": "Anonymous",
            "customer_id": "UNKNOWN",
        }
        for col, default in fill_rules.items():
            if col in df.columns:
                df[col] = df[col].fillna(default)

        self._log(
            "handle_missing_values", before, len(df),
            f"Quarantined {before - len(df)} rows missing critical fields; filled non-critical",
        )
        return df

    # ------------------------------------------------------------------ #
    # Step 7: Numeric range validation  — with quarantine
    # ------------------------------------------------------------------ #
    def validate_numeric_ranges(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Rejection rules (documented):
          quantity  must be > 0
          unit_price must be > 0
          discount   clipped to [0.0, 1.0] (soft correction, not rejection)
        """
        df = df.copy()
        before = len(df)

        invalid_qty = df["quantity"] <= 0 if "quantity" in df.columns else pd.Series(False, index=df.index)
        invalid_price = df["unit_price"] <= 0 if "unit_price" in df.columns else pd.Series(False, index=df.index)
        reject_mask = invalid_qty | invalid_price

        rejected = df[reject_mask].copy()
        rejected["rejection_reason"] = rejected.apply(
            lambda row: "; ".join(filter(None, [
                f"quantity={row.get('quantity')} ≤ 0" if pd.notna(row.get("quantity")) and row.get("quantity", 1) <= 0 else "",
                f"unit_price={row.get('unit_price')} ≤ 0" if pd.notna(row.get("unit_price")) and row.get("unit_price", 1) <= 0 else "",
            ])),
            axis=1,
        )
        self.quarantine.extend(rejected.to_dict("records"))
        df = df[~reject_mask].copy()

        if "discount" in df.columns:
            df["discount"] = df["discount"].clip(0.0, 1.0)

        self._log(
            "validate_numeric_ranges", before, len(df),
            f"Quarantined {before - len(df)} rows with invalid qty/price; clipped discount to [0,1]",
        )
        return df

    # ------------------------------------------------------------------ #
    # Step 8: Business-key deduplication  (original addition)
    # ------------------------------------------------------------------ #
    def deduplicate_by_business_key(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Business-key deduplication rule (documented):
          The business key is order_id.  When duplicates on order_id exist,
          keep the LAST occurrence (most recently ingested).

        This differs from the reference project which uses exact-row
        deduplication (pandas drop_duplicates with no subset argument).
        Business-key deduplication is more appropriate for real ETL pipelines
        because two rows can share an order_id but differ in other fields
        (e.g., status updates).
        """
        df = df.copy()
        before = len(df)
        key_cols = [k for k in BUSINESS_KEY if k in df.columns]
        if not key_cols:
            self._log("deduplicate_by_business_key", before, len(df), "No key columns found; skipped")
            return df

        dupes = df[df.duplicated(subset=key_cols, keep=False)].copy()
        dupes["rejection_reason"] = f"Duplicate business key ({', '.join(key_cols)})"
        # Keep last, quarantine the earlier ones
        first_dupes = dupes[dupes.duplicated(subset=key_cols, keep="last")]
        self.quarantine.extend(first_dupes.to_dict("records"))

        df = df.drop_duplicates(subset=key_cols, keep="last")
        self._log(
            "deduplicate_by_business_key", before, len(df),
            f"Removed {before - len(df)} duplicate order_ids (kept last occurrence)",
        )
        return df

    # ------------------------------------------------------------------ #
    # Step 9: Derived / computed columns
    # ------------------------------------------------------------------ #
    def compute_derived_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Derived columns:
          gross_amount    = unit_price × quantity
          discount_amount = gross_amount × discount
          net_amount      = gross_amount − discount_amount  (total sales)
          order_year / order_month / order_quarter / order_month_name
          is_discounted   = 1 if discount > 0 else 0
        """
        df = df.copy()
        df["gross_amount"] = (df["unit_price"] * df["quantity"]).round(2)
        df["discount_amount"] = (df["gross_amount"] * df["discount"]).round(2)
        df["net_amount"] = (df["gross_amount"] - df["discount_amount"]).round(2)
        df["order_year"] = df["order_date"].dt.year.astype("Int64")
        df["order_month"] = df["order_date"].dt.month.astype("Int64")
        df["order_quarter"] = df["order_date"].dt.quarter.astype("Int64")
        df["order_month_name"] = df["order_date"].dt.strftime("%B")
        df["is_discounted"] = (df["discount"] > 0).astype(int)
        self._log(
            "compute_derived_columns", len(df), len(df),
            "Added: gross/net/discount amounts, year/month/quarter, is_discounted",
        )
        return df

    # ------------------------------------------------------------------ #
    # Step 10: Final sort
    # ------------------------------------------------------------------ #
    def final_sort(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.sort_values("order_date").reset_index(drop=True)
        self._log("final_sort", len(df), len(df), "Sorted by order_date ascending")
        return df

    # ------------------------------------------------------------------ #
    # Quarantine export  (original addition)
    # ------------------------------------------------------------------ #
    def get_quarantine_df(self) -> pd.DataFrame:
        """Return all rejected records as a DataFrame."""
        if not self.quarantine:
            return pd.DataFrame()
        return pd.DataFrame(self.quarantine)

    # ------------------------------------------------------------------ #
    # Cleaning summary
    # ------------------------------------------------------------------ #
    def get_cleaning_summary(self, raw_count: int, clean_count: int) -> dict:
        """Return a dict summarising what the cleaning pipeline did."""
        return {
            "raw_records": raw_count,
            "clean_records": clean_count,
            "rejected_records": len(self.quarantine),
            "removal_count": raw_count - clean_count,
            "removal_pct": round((raw_count - clean_count) / raw_count * 100, 2) if raw_count > 0 else 0.0,
            "pipeline_steps": self.audit,
        }

    # ------------------------------------------------------------------ #
    # Audit report
    # ------------------------------------------------------------------ #
    def audit_report(self) -> pd.DataFrame:
        return pd.DataFrame(self.audit)

    # ------------------------------------------------------------------ #
    # Private helpers
    # ------------------------------------------------------------------ #
    def _log(self, step: str, rows_before: int, rows_after: int, note: str):
        entry = {
            "step": step,
            "rows_before": rows_before,
            "rows_after": rows_after,
            "rows_removed": rows_before - rows_after,
            "note": note,
        }
        self.audit.append(entry)
        logger.info(
            "[TRANSFORM] %-35s | %6d -> %6d | %s",
            step, rows_before, rows_after, note,
        )

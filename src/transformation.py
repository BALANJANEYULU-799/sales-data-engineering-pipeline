"""
src/transformation.py
=====================
Core ETL Transformation Stage: Schema standardization, multi-format parsing,
categorical normalization, rule-based quarantine routing, business-key
deduplication, financial metric computation, and audit logging.

Design Architecture:
--------------------
The Transformer class executes an 8-phase deterministic data transformation
pipeline. Every stage records row-level telemetry into an internal audit trail.
Records violating fundamental business constraints (missing primary identifiers,
non-positive financial attributes, duplicate primary keys) are segregated into
a structured quarantine collection with diagnostic error messages, preventing
corrupt records from polluting downstream analytical storage.
"""

import logging
import re
from typing import Dict, List, Optional, Tuple, Any

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# RFC-compliant email verification pattern
EMAIL_REGEX = re.compile(r"^[\w.+-]+@[\w-]+\.[a-zA-Z]{2,}$")

# Business Domain Whitelists
ALLOWED_STATUSES = {"Completed", "Pending", "Cancelled", "Refunded"}
ALLOWED_REGIONS = {"North", "South", "East", "West", "Central"}

# Primary business key for deduplication
BUSINESS_IDENTIFIER = ["order_id"]


class Transformer:
    """
    Executes modular data cleaning and transformation for sales transactions.

    Maintains complete data lineage through:
    1. An audit trail capturing row deltas across every transformation step.
    2. A structured quarantine store capturing rejected rows and failure reasons.
    """

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize the Transformer instance.

        Parameters
        ----------
        config : Optional[Dict[str, Any]]
            Optional configuration dictionary for pipeline tuning.
        """
        self.config = config or {}
        self.audit: List[Dict[str, Any]] = []
        self.quarantine: List[Dict[str, Any]] = []

    # ------------------------------------------------------------------ #
    # Master Execution Pipeline
    # ------------------------------------------------------------------ #
    def run(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Execute the full sequence of data cleaning and enrichment steps.

        Parameters
        ----------
        df : pd.DataFrame
            Raw extracted sales transactions.

        Returns
        -------
        pd.DataFrame
            Cleaned, validated, and enriched DataFrame ready for analytical loading.
        """
        initial_count = len(df)
        logger.info("[TRANSFORM] Initializing pipeline with %d input transactions", initial_count)

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

        removed_count = initial_count - len(df)
        logger.info(
            "[TRANSFORM] Pipeline completed successfully: %d clean records retained (%d quarantined/removed)",
            len(df),
            removed_count,
        )
        return df

    # ------------------------------------------------------------------ #
    # Step 1: Column Schema Standardization
    # ------------------------------------------------------------------ #
    def normalise_column_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Standardize DataFrame column headers to lowercase snake_case without whitespace.
        """
        df = df.copy()
        df.columns = [str(col).strip().lower().replace(" ", "_") for col in df.columns]
        self._record_step("normalise_column_names", len(df), len(df), "Standardized column headers to snake_case")
        return df

    # ------------------------------------------------------------------ #
    # Step 2: Temporal and Numeric Type Casting
    # ------------------------------------------------------------------ #
    def cast_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Parse order_date using mixed date format detection (ISO YYYY-MM-DD and DD/MM/YYYY).
        Cast numerical fields (quantity, unit_price, discount) with numeric coercion.
        """
        df = df.copy()
        start_rows = len(df)

        # Parse mixed date representations safely
        if "order_date" in df.columns:
            df["order_date"] = pd.to_datetime(df["order_date"], format="mixed", errors="coerce")

        for numeric_col in ["quantity", "unit_price", "discount"]:
            if numeric_col in df.columns:
                df[numeric_col] = pd.to_numeric(df[numeric_col], errors="coerce")

        self._record_step(
            "cast_types",
            start_rows,
            len(df),
            "Casting completed: mixed datetime format resolution and numeric coercion",
        )
        return df

    # ------------------------------------------------------------------ #
    # Step 3: Order Status Normalization
    # ------------------------------------------------------------------ #
    def clean_status(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Standardize status values to Title Case.
        Unrecognized status values are coerced to NaN and later filled with 'Pending'.
        """
        df = df.copy()
        if "status" not in df.columns:
            return df

        df["status"] = df["status"].astype(str).str.strip().str.title()
        invalid_mask = ~df["status"].isin(ALLOWED_STATUSES) & df["status"].notna()
        df.loc[invalid_mask, "status"] = np.nan

        self._record_step(
            "clean_status",
            len(df),
            len(df),
            f"Status normalized; {int(invalid_mask.sum())} unverified status values flagged for default assignment",
        )
        return df

    # ------------------------------------------------------------------ #
    # Step 4: Geographic Region Normalization
    # ------------------------------------------------------------------ #
    def clean_region(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Normalize geographic regions: strip whitespace, title-case, and map invalid tokens to NaN.
        """
        df = df.copy()
        if "region" not in df.columns:
            return df

        df["region"] = df["region"].replace({"N/A": np.nan, "NA": np.nan, "": np.nan})
        df["region"] = df["region"].astype(str).str.strip().str.title()
        invalid_mask = ~df["region"].isin(ALLOWED_REGIONS) & df["region"].notna()
        df.loc[invalid_mask, "region"] = np.nan

        self._record_step(
            "clean_region",
            len(df),
            len(df),
            f"Geographic regions standardized against canonical zones",
        )
        return df

    # ------------------------------------------------------------------ #
    # Step 5: Customer Email Verification
    # ------------------------------------------------------------------ #
    def validate_email(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Validate email syntax against standard regex pattern.
        Malformed emails are set to NaN (soft sanitization rather than row rejection).
        """
        if "email" not in df.columns:
            return df
        df = df.copy()

        def is_valid_email(val: Any) -> bool:
            if pd.isna(val) or val is None:
                return True
            return bool(EMAIL_REGEX.match(str(val).strip()))

        invalid_mask = ~df["email"].apply(is_valid_email)
        df.loc[invalid_mask, "email"] = np.nan

        self._record_step(
            "validate_email",
            len(df),
            len(df),
            f"Sanitized customer contact emails: {int(invalid_mask.sum())} invalid emails reset to NaN",
        )
        return df

    # ------------------------------------------------------------------ #
    # Step 6: Missing Field Policy & Quarantine Routing
    # ------------------------------------------------------------------ #
    def handle_missing_values(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Enforce primary attribute integrity:
        - Critical business columns (order_id, product, unit_price, order_date) cannot be null.
          Violating rows are routed to the quarantine queue with clear diagnostic annotations.
        - Non-critical attributes receive deterministic domain defaults:
          status -> 'Pending', discount -> 0.0, region -> 'Unknown',
          customer_name -> 'Anonymous', customer_id -> 'UNKNOWN'.
        """
        df = df.copy()
        start_rows = len(df)

        mandatory_fields = ["order_id", "product", "unit_price", "order_date"]
        active_mandatory = [col for col in mandatory_fields if col in df.columns]

        missing_mask = df[active_mandatory].isnull().any(axis=1)
        if missing_mask.any():
            quarantined_subset = df[missing_mask].copy()
            quarantined_subset["rejection_reason"] = quarantined_subset[active_mandatory].apply(
                lambda row: "Missing critical field(s): "
                + ", ".join(col for col in active_mandatory if pd.isna(row[col])),
                axis=1,
            )
            self.quarantine.extend(quarantined_subset.to_dict("records"))
            df = df[~missing_mask].copy()

        # Deterministic default assignments for non-essential attributes
        fallback_mappings = {
            "status": "Pending",
            "discount": 0.0,
            "region": "Unknown",
            "customer_name": "Anonymous",
            "customer_id": "UNKNOWN",
        }
        for col, default_val in fallback_mappings.items():
            if col in df.columns:
                df[col] = df[col].fillna(default_val)

        quarantined_count = start_rows - len(df)
        self._record_step(
            "handle_missing_values",
            start_rows,
            len(df),
            f"Quarantined {quarantined_count} records lacking essential keys; imputed defaults for secondary attributes",
        )
        return df

    # ------------------------------------------------------------------ #
    # Step 7: Domain Range Bounds & Business Rules
    # ------------------------------------------------------------------ #
    def validate_numeric_ranges(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Validate positive domain constraints:
        - quantity must be > 0
        - unit_price must be > 0
        - discount must fall in range [0.0, 1.0]; values outside are clipped.
        Records violating quantity or price bounds are quarantined.
        """
        df = df.copy()
        start_rows = len(df)

        invalid_qty = df["quantity"] <= 0 if "quantity" in df.columns else pd.Series(False, index=df.index)
        invalid_price = df["unit_price"] <= 0 if "unit_price" in df.columns else pd.Series(False, index=df.index)
        violation_mask = invalid_qty | invalid_price

        if violation_mask.any():
            rejected_subset = df[violation_mask].copy()
            rejected_subset["rejection_reason"] = rejected_subset.apply(
                lambda row: "; ".join(filter(None, [
                    f"quantity={row.get('quantity')} <= 0"
                    if pd.notna(row.get("quantity")) and row.get("quantity", 1) <= 0 else "",
                    f"unit_price={row.get('unit_price')} <= 0"
                    if pd.notna(row.get("unit_price")) and row.get("unit_price", 1) <= 0 else "",
                ])),
                axis=1,
            )
            self.quarantine.extend(rejected_subset.to_dict("records"))
            df = df[~violation_mask].copy()

        # Soft boundary clamp for promotional discount rate
        if "discount" in df.columns:
            df["discount"] = df["discount"].clip(0.0, 1.0)

        quarantined_count = start_rows - len(df)
        self._record_step(
            "validate_numeric_ranges",
            start_rows,
            len(df),
            f"Quarantined {quarantined_count} transactions with invalid quantity/price; constrained discount rate to [0.0, 1.0]",
        )
        return df

    # ------------------------------------------------------------------ #
    # Step 8: Business-Key Deduplication
    # ------------------------------------------------------------------ #
    def deduplicate_by_business_key(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Resolve primary business key collisions on order_id.
        Policy: retain the most recently received transaction ('last') and quarantine
        superseded or redundant prior entries.
        """
        df = df.copy()
        start_rows = len(df)
        active_keys = [k for k in BUSINESS_IDENTIFIER if k in df.columns]

        if not active_keys:
            self._record_step("deduplicate_by_business_key", start_rows, len(df), "No key columns detected; skipping")
            return df

        duplicate_mask = df.duplicated(subset=active_keys, keep=False)
        if duplicate_mask.any():
            dupes = df[duplicate_mask].copy()
            dupes["rejection_reason"] = f"Duplicate business key ({', '.join(active_keys)})"
            # Retain last occurrence, quarantine preceding collisions
            preceding_dupes = dupes[dupes.duplicated(subset=active_keys, keep="last")]
            self.quarantine.extend(preceding_dupes.to_dict("records"))

        df = df.drop_duplicates(subset=active_keys, keep="last")
        deduped_count = start_rows - len(df)
        self._record_step(
            "deduplicate_by_business_key",
            start_rows,
            len(df),
            f"Removed {deduped_count} duplicate transactions sharing order_id (preserved most recent state)",
        )
        return df

    # ------------------------------------------------------------------ #
    # Step 9: Financial Metrics & Temporal Feature Engineering
    # ------------------------------------------------------------------ #
    def compute_derived_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Compute transactional accounting figures and analytical time hierarchies:
        - gross_amount    = unit_price * quantity
        - discount_amount = gross_amount * discount
        - net_amount      = gross_amount - discount_amount
        - order_year, order_month, order_quarter, order_month_name
        - is_discounted   = 1 if discount > 0 else 0
        """
        df = df.copy()
        df["gross_amount"] = (df["unit_price"] * df["quantity"]).round(2)
        df["discount_amount"] = (df["gross_amount"] * df["discount"]).round(2)
        df["net_amount"] = (df["gross_amount"] - df["discount_amount"]).round(2)

        if "order_date" in df.columns and pd.api.types.is_datetime64_any_dtype(df["order_date"]):
            df["order_year"] = df["order_date"].dt.year.astype("Int64")
            df["order_month"] = df["order_date"].dt.month.astype("Int64")
            df["order_quarter"] = df["order_date"].dt.quarter.astype("Int64")
            df["order_month_name"] = df["order_date"].dt.strftime("%B")

        if "discount" in df.columns:
            df["is_discounted"] = (df["discount"] > 0).astype(int)

        self._record_step(
            "compute_derived_columns",
            len(df),
            len(df),
            "Engineered revenue metrics (gross, discount, net) and calendar dimension attributes",
        )
        return df

    # ------------------------------------------------------------------ #
    # Step 10: Deterministic Chronological Ordering
    # ------------------------------------------------------------------ #
    def final_sort(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Sort transactions chronologically by order_date ascending with reset index.
        """
        if "order_date" in df.columns:
            df = df.sort_values("order_date").reset_index(drop=True)
        self._record_step("final_sort", len(df), len(df), "Sorted dataset chronologically by order_date")
        return df

    # ------------------------------------------------------------------ #
    # Operational Artifact Accessors & Reporting
    # ------------------------------------------------------------------ #
    def get_quarantine_df(self) -> pd.DataFrame:
        """Return all quarantined records as a pandas DataFrame."""
        if not self.quarantine:
            return pd.DataFrame()
        return pd.DataFrame(self.quarantine)

    def get_cleaning_summary(self, raw_count: int, clean_count: int) -> Dict[str, Any]:
        """
        Produce a structured operational summary of the transformation execution.
        """
        rejected_total = len(self.quarantine)
        drop_delta = raw_count - clean_count
        rejection_rate = round((drop_delta / raw_count * 100), 2) if raw_count > 0 else 0.0

        return {
            "raw_records": raw_count,
            "clean_records": clean_count,
            "rejected_records": rejected_total,
            "removal_count": drop_delta,
            "removal_pct": rejection_rate,
            "pipeline_steps": self.audit,
        }

    def audit_report(self) -> pd.DataFrame:
        """Return the step-by-step audit lineage as a pandas DataFrame."""
        return pd.DataFrame(self.audit)

    def _record_step(self, step: str, rows_before: int, rows_after: int, note: str):
        """Append an execution entry to the audit log and emit logging message."""
        entry = {
            "step": step,
            "rows_before": rows_before,
            "rows_after": rows_after,
            "rows_removed": rows_before - rows_after,
            "note": note,
        }
        self.audit.append(entry)
        logger.info("[TRANSFORM] %-32s | %6d -> %6d | %s", step, rows_before, rows_after, note)

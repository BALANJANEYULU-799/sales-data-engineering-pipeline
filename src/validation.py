"""
src/validation.py
=================
Data Quality Validation Stage: Runs automated pre-load verification checks
against the transformed dataset to guarantee structural, financial, and relational integrity.
Generates an auditable Data Quality JSON scorecard with pass/fail metrics.
"""

import logging
from datetime import datetime

import pandas as pd

logger = logging.getLogger(__name__)


class DataQualityValidator:
    """
    Runs data quality checks on a DataFrame and records results.

    Each check appends a result dict to self.results with:
        check_name  : str  – human-readable rule name
        description : str  – what the check verifies
        records_checked : int
        failures    : int  – number of records failing the check
        passed      : bool – True if failures == 0
    """

    def __init__(self):
        self.results: list[dict] = []

    def run_all_checks(self, df: pd.DataFrame) -> list[dict]:
        """Run all DQ checks and return the results list."""
        logger.info("[VALIDATE] Running data quality checks on %d rows …", len(df))

        self._check_required_columns(df)
        self._check_no_missing_order_id(df)
        self._check_no_missing_product(df)
        self._check_no_missing_price(df)
        self._check_no_missing_date(df)
        self._check_positive_quantity(df)
        self._check_positive_price(df)
        self._check_discount_range(df)
        self._check_no_negative_net_amount(df)
        self._check_valid_status(df)
        self._check_order_id_uniqueness(df)
        self._check_net_amount_consistency(df)
        self._check_date_not_future(df)

        passed = sum(1 for r in self.results if r["passed"])
        failed = len(self.results) - passed
        logger.info("[VALIDATE] %d checks: %d passed, %d failed", len(self.results), passed, failed)
        return self.results

    def get_report(self, df: pd.DataFrame) -> dict:
        """Generate the full DQ report dict (call after run_all_checks)."""
        passed = sum(1 for r in self.results if r["passed"])
        return {
            "generated_at": datetime.now().isoformat(),
            "rows_validated": len(df),
            "total_checks": len(self.results),
            "checks_passed": passed,
            "checks_failed": len(self.results) - passed,
            "overall_status": "PASS" if all(r["passed"] for r in self.results) else "FAIL",
            "checks": self.results,
        }

    # ------------------------------------------------------------------ #
    # Individual checks
    # ------------------------------------------------------------------ #
    def _check_required_columns(self, df: pd.DataFrame):
        required = ["order_id", "product", "category", "quantity",
                    "unit_price", "order_date", "region", "status",
                    "gross_amount", "net_amount"]
        missing = [c for c in required if c not in df.columns]
        self._record(
            check_name="required_columns_present",
            description=f"All required columns exist: {required}",
            records_checked=len(df),
            failures=len(missing),
            detail=f"Missing: {missing}" if missing else "All present",
        )

    def _check_no_missing_order_id(self, df: pd.DataFrame):
        failures = int(df["order_id"].isna().sum()) if "order_id" in df.columns else len(df)
        self._record("no_missing_order_id", "order_id must not be NULL", len(df), failures)

    def _check_no_missing_product(self, df: pd.DataFrame):
        failures = int(df["product"].isna().sum()) if "product" in df.columns else len(df)
        self._record("no_missing_product", "product must not be NULL", len(df), failures)

    def _check_no_missing_price(self, df: pd.DataFrame):
        failures = int(df["unit_price"].isna().sum()) if "unit_price" in df.columns else len(df)
        self._record("no_missing_unit_price", "unit_price must not be NULL", len(df), failures)

    def _check_no_missing_date(self, df: pd.DataFrame):
        failures = int(df["order_date"].isna().sum()) if "order_date" in df.columns else len(df)
        self._record("no_missing_order_date", "order_date must not be NULL", len(df), failures)

    def _check_positive_quantity(self, df: pd.DataFrame):
        if "quantity" not in df.columns:
            return
        failures = int((df["quantity"] <= 0).sum())
        self._record("positive_quantity", "quantity must be > 0", len(df), failures)

    def _check_positive_price(self, df: pd.DataFrame):
        if "unit_price" not in df.columns:
            return
        failures = int((df["unit_price"] <= 0).sum())
        self._record("positive_unit_price", "unit_price must be > 0", len(df), failures)

    def _check_discount_range(self, df: pd.DataFrame):
        if "discount" not in df.columns:
            return
        failures = int(((df["discount"] < 0) | (df["discount"] > 1)).sum())
        self._record("discount_in_range_0_to_1", "discount must be between 0.0 and 1.0", len(df), failures)

    def _check_no_negative_net_amount(self, df: pd.DataFrame):
        if "net_amount" not in df.columns:
            return
        failures = int((df["net_amount"] < 0).sum())
        self._record("no_negative_net_amount", "net_amount must be >= 0", len(df), failures)

    def _check_valid_status(self, df: pd.DataFrame):
        if "status" not in df.columns:
            return
        valid = {"Completed", "Pending", "Cancelled", "Refunded"}
        failures = int((~df["status"].isin(valid)).sum())
        self._record(
            "valid_status_values",
            f"status must be one of {sorted(valid)}",
            len(df), failures,
        )

    def _check_order_id_uniqueness(self, df: pd.DataFrame):
        if "order_id" not in df.columns:
            return
        failures = int(df.duplicated(subset=["order_id"]).sum())
        self._record(
            "order_id_unique",
            "order_id must be unique (no duplicate business keys)",
            len(df), failures,
        )

    def _check_net_amount_consistency(self, df: pd.DataFrame):
        """net_amount should equal gross_amount - discount_amount (±1 cent rounding)."""
        required = ["gross_amount", "discount_amount", "net_amount"]
        if not all(c in df.columns for c in required):
            return
        computed = (df["gross_amount"] - df["discount_amount"]).round(2)
        failures = int((abs(computed - df["net_amount"]) > 0.01).sum())
        self._record(
            "net_amount_consistency",
            "net_amount = gross_amount - discount_amount (±0.01 tolerance)",
            len(df), failures,
        )

    def _check_date_not_future(self, df: pd.DataFrame):
        """order_date should not be in the future."""
        if "order_date" not in df.columns:
            return
        now = pd.Timestamp.now()
        failures = int((df["order_date"] > now).sum())
        self._record(
            "date_not_in_future",
            "order_date must not be in the future",
            len(df), failures,
        )

    # ------------------------------------------------------------------ #
    # Result helper
    # ------------------------------------------------------------------ #
    def _record(
        self,
        check_name: str,
        description: str,
        records_checked: int,
        failures: int,
        detail: str = "",
    ):
        passed = failures == 0
        result = {
            "check_name": check_name,
            "description": description,
            "records_checked": records_checked,
            "failures": failures,
            "passed": passed,
            "status": "PASS" if passed else "FAIL",
        }
        if detail:
            result["detail"] = detail
        self.results.append(result)
        status_str = "✓ PASS" if passed else f"✗ FAIL ({failures} failures)"
        logger.info("[VALIDATE] %-40s %s", check_name, status_str)

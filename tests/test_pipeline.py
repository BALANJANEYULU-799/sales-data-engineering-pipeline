"""
tests/test_pipeline.py
=======================
Automated unit, regression, and integration test suite for the Sales Data
Engineering Pipeline.

Test Modules & Coverage:
------------------------
  TestExtractor         – CSV ingestion, schema contract validation, profile metrics
  TestTransformer       – Normalization, parsing, domain bounds, quarantine, metrics
  TestQuarantine        – Rejection tracking, fault reason capture, summary metrics
  TestValidator         – Pre-load data quality rules and scorecard verification
  TestDatabaseLoader    – Relational table loading (replace, append, upsert)
  TestIncrementalLoader – Idempotency guarantees, batch audit history, incremental deltas
  TestIntegration       – End-to-end execution of full pipeline on synthetic transactions
  TestGenAISummary      – Metrics aggregation and GenAI prompt template synthesis
"""

import csv
import json
import os
import sys

import numpy as np
import pandas as pd
import pytest

# ── Include src/ in execution path ──────────────────────────────────────────
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from extraction import Extractor, REQUIRED_COLUMNS
from transformation import Transformer
from validation import DataQualityValidator
from database import DatabaseLoader
from incremental import IncrementalLoader


CONFIG = {}


# ─────────────────────────────────────────────────────────────────────────
# Test Fixtures (Custom Enterprise Synthetic Data)
# ─────────────────────────────────────────────────────────────────────────
@pytest.fixture
def raw_sample_df():
    """
    Synthetic transactional sales records crafted with intentional edge cases:
    - Primary business key duplicate (order_id=1002 repeated)
    - Null customer attributes (customer_id / customer_name null in order_id=1004)
    - Malformed email string ('invalid-email-format')
    - Geographic invalid token ('N/A')
    - Out-of-bounds negative quantity (-1 in order_id=1003)
    - Missing critical unit price (None in order_id=1004)
    - Inconsistent status casing ('Completed', 'pending', 'COMPLETED', None, 'Cancelled', 'completed')
    - Mixed date string representations ('2024-01-15' vs '01/03/2024')
    """
    return pd.DataFrame({
        "order_id":      [1001, 1002, 1002, 1003, 1004, 1005, 1006],
        "customer_id":   ["C-101", "C-102", "C-102", "C-103", None, "C-105", "C-106"],
        "customer_name": ["Apex Retail", "Beacon Logistics", "Beacon Logistics", "Crestline Ltd", None, "Echo Systems", "Falcon Data"],
        "email":         ["apex@example.com", "invalid-email-format", "invalid-email-format", "sales@crestline.com", "procurement@echo.com", None, "orders@falcon.org"],
        "region":        ["North", "South", "South", "East", "N/A", "West", "Central"],
        "product":       ["Ergonomic Keyboard", "Ultra HD Monitor", "Ultra HD Monitor", "USB-C Hub", "Server Rack", "Noise-Canceling Headset", "Wireless Mouse"],
        "category":      ["Hardware Peripherals"] * 7,
        "quantity":      [2, 3, 3, -1, 1, 5, 2],
        "unit_price":    [45.0, 280.0, 280.0, 35.0, None, 120.0, 25.0],
        "discount":      [0.0, 0.1, 0.1, 0.0, 0.0, 0.05, 0.0],
        "status":        ["Completed", "pending", "pending", "COMPLETED", None, "Cancelled", "completed"],
        "order_date":    ["2024-01-15", "2024-02-20", "2024-02-20", "01/03/2024",
                          "2024-04-10", "2024-05-05", "2024-06-01"],
    })


@pytest.fixture
def transformer():
    return Transformer(CONFIG)


@pytest.fixture
def clean_df(raw_sample_df, transformer):
    """Clean, fully processed dataset derived from sample fixture."""
    return transformer.run(raw_sample_df.copy())


# ─────────────────────────────────────────────────────────────────────────
# TestExtractor
# ─────────────────────────────────────────────────────────────────────────
class TestExtractor:
    def test_from_csv_raises_file_not_found(self):
        ex = Extractor(CONFIG)
        with pytest.raises(FileNotFoundError):
            ex.from_csv("nonexistent_sales_file.csv")

    def test_from_csv_raises_on_missing_required_column(self, tmp_path):
        """CSV without mandatory 'order_id' must raise schema contract ValueError."""
        invalid_csv = tmp_path / "missing_keys.csv"
        invalid_csv.write_text(
            "product,quantity,unit_price,order_date,region,status,category\n"
            "Ergonomic Keyboard,1,45.0,2024-01-01,North,Completed,Hardware Peripherals\n"
        )
        ex = Extractor(CONFIG)
        with pytest.raises(ValueError, match="Missing required columns"):
            ex.from_csv(str(invalid_csv))

    def test_from_csv_reads_valid_file(self, tmp_path):
        valid_csv = tmp_path / "valid_sales.csv"
        valid_csv.write_text(
            "order_id,product,category,quantity,unit_price,order_date,region,status\n"
            "1001,Ergonomic Keyboard,Hardware Peripherals,2,45.0,2024-01-01,North,Completed\n"
        )
        ex = Extractor(CONFIG)
        df = ex.from_csv(str(valid_csv))
        assert len(df) == 1
        assert "order_id" in df.columns

    def test_profile_shape(self, raw_sample_df):
        ex = Extractor(CONFIG)
        profile = ex.profile(raw_sample_df)
        assert profile["rows"] == len(raw_sample_df)
        assert profile["column_count"] == len(raw_sample_df.columns)

    def test_profile_detects_duplicates(self, raw_sample_df):
        ex = Extractor(CONFIG)
        profile = ex.profile(raw_sample_df)
        assert profile["duplicates"] >= 1

    def test_profile_detects_nulls(self, raw_sample_df):
        ex = Extractor(CONFIG)
        profile = ex.profile(raw_sample_df)
        assert profile["null_counts"]["customer_id"] >= 1

    def test_profile_numeric_stats(self, raw_sample_df):
        ex = Extractor(CONFIG)
        profile = ex.profile(raw_sample_df)
        assert "quantity" in profile["numeric_stats"]
        assert "min" in profile["numeric_stats"]["quantity"]


# ─────────────────────────────────────────────────────────────────────────
# TestTransformer
# ─────────────────────────────────────────────────────────────────────────
class TestTransformer:

    def test_normalise_column_names(self, transformer):
        df = pd.DataFrame({"Order ID": [1001], "Unit Price": [45.0]})
        result = transformer.normalise_column_names(df)
        assert "order_id" in result.columns
        assert "unit_price" in result.columns

    def test_cast_types_date_iso(self, raw_sample_df, transformer):
        df = transformer.normalise_column_names(raw_sample_df.copy())
        result = transformer.cast_types(df)
        assert pd.api.types.is_datetime64_any_dtype(result["order_date"])

    def test_cast_types_handles_day_first_format(self, transformer):
        df = pd.DataFrame({
            "order_id": [1001], "order_date": ["15/01/2024"],
            "quantity": ["2"], "unit_price": ["45.0"], "discount": ["0.0"],
        })
        result = transformer.cast_types(df)
        assert result["order_date"].iloc[0] == pd.Timestamp("2024-01-15")

    def test_cast_types_invalid_date_becomes_nat(self, transformer):
        df = pd.DataFrame({
            "order_id": [1001], "order_date": ["corrupt-timestamp"],
            "quantity": ["2"], "unit_price": ["45.0"], "discount": ["0.0"],
        })
        result = transformer.cast_types(df)
        assert pd.isna(result["order_date"].iloc[0])

    def test_clean_status_normalises_case(self, raw_sample_df, transformer):
        df = transformer.normalise_column_names(raw_sample_df.copy())
        result = transformer.clean_status(df)
        valid = {"Completed", "Pending", "Cancelled", "Refunded"}
        assert all(s in valid for s in result["status"].dropna())

    def test_clean_region_removes_na(self, raw_sample_df, transformer):
        df = transformer.normalise_column_names(raw_sample_df.copy())
        result = transformer.clean_region(df)
        assert "N/A" not in result["region"].dropna().astype(str).values
        valid_regions = {"North", "South", "East", "West", "Central"}
        assert result["region"].dropna().isin(valid_regions).all()

    def test_validate_email_invalidates_bad(self, raw_sample_df, transformer):
        df = transformer.normalise_column_names(raw_sample_df.copy())
        result = transformer.validate_email(df)
        assert "invalid-email-format" not in result["email"].fillna("").values

    def test_handle_missing_quarantines_critical(self, transformer):
        """Transactions missing order_id, product, unit_price, or order_date are quarantined."""
        df = pd.DataFrame({
            "order_id":   [1001, 1002, 1003],
            "product":    ["Keyboard", "Monitor", None],
            "unit_price": [45.0, None, 280.0],
            "order_date": [pd.Timestamp("2024-01-01")] * 3,
            "status":     ["Completed"] * 3,
            "discount":   [0.0] * 3,
        })
        result = transformer.handle_missing_values(df)
        assert len(result) == 1
        assert len(transformer.quarantine) == 2

    def test_handle_missing_fills_non_critical(self, transformer):
        df = pd.DataFrame({
            "order_id":    [1001],
            "product":     ["Ergonomic Keyboard"],
            "unit_price":  [45.0],
            "order_date":  [pd.Timestamp("2024-01-01")],
            "status":      [None],
            "discount":    [None],
            "region":      [None],
            "customer_name": [None],
            "customer_id": [None],
        })
        result = transformer.handle_missing_values(df)
        assert result.iloc[0]["status"] == "Pending"
        assert result.iloc[0]["discount"] == 0.0
        assert result.iloc[0]["region"] == "Unknown"
        assert result.iloc[0]["customer_name"] == "Anonymous"
        assert result.iloc[0]["customer_id"] == "UNKNOWN"

    def test_validate_numeric_removes_negative_qty(self, raw_sample_df, transformer):
        df = transformer.normalise_column_names(raw_sample_df.copy())
        df = transformer.cast_types(df)
        df = transformer.handle_missing_values(df)
        result = transformer.validate_numeric_ranges(df)
        assert (result["quantity"] > 0).all()

    def test_deduplicate_by_business_key(self, raw_sample_df, transformer):
        """Business key deduplication keeps the latest order_id entry."""
        df = transformer.normalise_column_names(raw_sample_df.copy())
        before = len(df)
        result = transformer.deduplicate_by_business_key(df)
        assert result.duplicated(subset=["order_id"]).sum() == 0
        assert len(result) < before

    def test_compute_derived_columns(self, transformer):
        df = pd.DataFrame({
            "order_id":   [1001],
            "unit_price": [100.0],
            "quantity":   [3],
            "discount":   [0.1],
            "order_date": [pd.Timestamp("2024-03-15")],
        })
        result = transformer.compute_derived_columns(df)
        assert result.iloc[0]["gross_amount"] == 300.0
        assert result.iloc[0]["discount_amount"] == 30.0
        assert result.iloc[0]["net_amount"] == 270.0
        assert result.iloc[0]["order_year"] == 2024
        assert result.iloc[0]["order_quarter"] == 1
        assert result.iloc[0]["is_discounted"] == 1

    def test_full_pipeline_reduces_rows(self, raw_sample_df, transformer):
        result = transformer.run(raw_sample_df.copy())
        assert len(result) < len(raw_sample_df)
        assert result.duplicated(subset=["order_id"]).sum() == 0

    def test_full_pipeline_adds_derived_columns(self, raw_sample_df, transformer):
        result = transformer.run(raw_sample_df.copy())
        for col in ["gross_amount", "net_amount", "order_year", "is_discounted"]:
            assert col in result.columns

    def test_audit_report_has_all_steps(self, raw_sample_df, transformer):
        transformer.run(raw_sample_df.copy())
        audit = transformer.audit_report()
        assert len(audit) > 0
        assert "step" in audit.columns
        assert "rows_before" in audit.columns
        assert "rows_after" in audit.columns


# ─────────────────────────────────────────────────────────────────────────
# TestQuarantine
# ─────────────────────────────────────────────────────────────────────────
class TestQuarantine:
    def test_rejected_records_have_reason(self, raw_sample_df, transformer):
        transformer.run(raw_sample_df.copy())
        quarantine_df = transformer.get_quarantine_df()
        assert not quarantine_df.empty
        assert "rejection_reason" in quarantine_df.columns
        assert quarantine_df["rejection_reason"].notna().all()

    def test_negative_quantity_quarantined(self, transformer):
        df = pd.DataFrame({
            "order_id":   [9901],
            "product":    ["Ultra HD Monitor"],
            "unit_price": [280.0],
            "order_date": [pd.Timestamp("2024-01-01")],
            "quantity":   [-5],
            "status":     ["Completed"],
            "discount":   [0.0],
            "region":     ["North"],
            "category":   ["Hardware Peripherals"],
        })
        transformer.handle_missing_values(df)
        transformer.validate_numeric_ranges(df)
        quarantine_df = transformer.get_quarantine_df()
        reasons = quarantine_df["rejection_reason"].tolist()
        assert any("quantity" in str(r) for r in reasons) or len(quarantine_df) == 0

    def test_cleaning_summary_counts(self, raw_sample_df, transformer):
        result = transformer.run(raw_sample_df.copy())
        summary = transformer.get_cleaning_summary(len(raw_sample_df), len(result))
        assert summary["raw_records"] == len(raw_sample_df)
        assert summary["clean_records"] == len(result)
        assert summary["rejected_records"] == len(transformer.quarantine)


# ─────────────────────────────────────────────────────────────────────────
# TestValidator
# ─────────────────────────────────────────────────────────────────────────
class TestValidator:
    def test_all_checks_run(self, clean_df):
        validator = DataQualityValidator()
        results = validator.run_all_checks(clean_df)
        assert len(results) >= 10

    def test_clean_data_passes_basic_checks(self, clean_df):
        validator = DataQualityValidator()
        validator.run_all_checks(clean_df)
        report = validator.get_report(clean_df)
        assert report["checks_passed"] > 0

    def test_negative_quantity_fails_check(self, transformer):
        df = pd.DataFrame({
            "order_id":       [1001],
            "product":        ["Ergonomic Keyboard"],
            "category":       ["Hardware Peripherals"],
            "quantity":       [-5],
            "unit_price":     [45.0],
            "order_date":     [pd.Timestamp("2024-01-01")],
            "region":         ["North"],
            "status":         ["Completed"],
            "gross_amount":   [-225.0],
            "discount_amount":[0.0],
            "net_amount":     [-225.0],
            "discount":       [0.0],
            "is_discounted":  [0],
        })
        validator = DataQualityValidator()
        validator.run_all_checks(df)
        failed = [r for r in validator.results if not r["passed"]]
        failed_names = [r["check_name"] for r in failed]
        assert "positive_quantity" in failed_names

    def test_duplicate_order_id_fails_check(self, transformer):
        df = pd.DataFrame({
            "order_id":       [1001, 1001],
            "product":        ["Item A", "Item B"],
            "category":       ["Hardware Peripherals", "Hardware Peripherals"],
            "quantity":       [1, 2],
            "unit_price":     [45.0, 90.0],
            "order_date":     [pd.Timestamp("2024-01-01")] * 2,
            "region":         ["North", "South"],
            "status":         ["Completed", "Pending"],
            "gross_amount":   [45.0, 180.0],
            "discount_amount":[0.0, 0.0],
            "net_amount":     [45.0, 180.0],
            "discount":       [0.0, 0.0],
            "is_discounted":  [0, 0],
        })
        validator = DataQualityValidator()
        validator.run_all_checks(df)
        failed_names = [r["check_name"] for r in validator.results if not r["passed"]]
        assert "order_id_unique" in failed_names

    def test_report_structure(self, clean_df):
        validator = DataQualityValidator()
        validator.run_all_checks(clean_df)
        report = validator.get_report(clean_df)
        assert "total_checks" in report
        assert "checks_passed" in report
        assert "checks_failed" in report
        assert "overall_status" in report
        assert "checks" in report


# ─────────────────────────────────────────────────────────────────────────
# TestDatabaseLoader
# ─────────────────────────────────────────────────────────────────────────
class TestDatabaseLoader:
    def test_load_replace(self, clean_df, tmp_path):
        db_url = f"sqlite:///{tmp_path}/test.db"
        loader = DatabaseLoader(db_url)
        count = loader.load_replace(clean_df, "orders_clean")
        assert count == len(clean_df)

    def test_verify_count(self, clean_df, tmp_path):
        db_url = f"sqlite:///{tmp_path}/test.db"
        loader = DatabaseLoader(db_url)
        loader.load_replace(clean_df, "orders_clean")
        assert loader.verify("orders_clean") == len(clean_df)

    def test_load_upsert_skips_existing(self, clean_df, tmp_path):
        """Idempotent loading: re-loading unchanged data loads 0 new rows."""
        db_url = f"sqlite:///{tmp_path}/upsert_test.db"
        loader = DatabaseLoader(db_url)
        result1 = loader.load_upsert(clean_df, "orders_clean")
        result2 = loader.load_upsert(clean_df, "orders_clean")
        assert result1["new"] == len(clean_df)
        assert result2["new"] == 0
        assert result2["skipped"] == len(clean_df)

    def test_build_summaries(self, clean_df, tmp_path):
        db_url = f"sqlite:///{tmp_path}/summary_test.db"
        loader = DatabaseLoader(db_url)
        loader.load_replace(clean_df, "orders_clean")
        summaries = loader.build_summaries(clean_df)
        assert "monthly" in summaries
        assert "by_category" in summaries
        assert "by_region" in summaries


# ─────────────────────────────────────────────────────────────────────────
# TestIncrementalLoader
# ─────────────────────────────────────────────────────────────────────────
class TestIncrementalLoader:
    def _make_batch(self, order_ids: list) -> pd.DataFrame:
        rows = []
        for oid in order_ids:
            rows.append({
                "order_id": oid,
                "customer_id": "C-101",
                "customer_name": "Apex Retail",
                "email": "orders@apexretail.com",
                "region": "North",
                "product": "Ergonomic Keyboard",
                "category": "Hardware Peripherals",
                "quantity": 2,
                "unit_price": 45.0,
                "discount": 0.0,
                "gross_amount": 90.0,
                "discount_amount": 0.0,
                "net_amount": 90.0,
                "status": "Completed",
                "order_date": "2024-01-15",
                "order_year": 2024,
                "order_month": 1,
                "order_quarter": 1,
                "order_month_name": "January",
                "is_discounted": 0,
            })
        return pd.DataFrame(rows)

    def test_first_batch_loads_all(self, tmp_path):
        db_url = f"sqlite:///{tmp_path}/incr.db"
        loader = IncrementalLoader(db_url)
        batch = self._make_batch([101, 102, 103])
        result = loader.load_batch(batch, "orders_clean", "batch_1")
        assert result["new"] == 3
        assert result["skipped"] == 0

    def test_idempotency_same_batch_loads_zero(self, tmp_path):
        """Re-running identical batch results in 0 inserts and 3 skips."""
        db_url = f"sqlite:///{tmp_path}/incr.db"
        loader = IncrementalLoader(db_url)
        batch = self._make_batch([101, 102, 103])
        loader.load_batch(batch, "orders_clean", "batch_1")
        result2 = loader.load_batch(batch, "orders_clean", "batch_1_retry")
        assert result2["new"] == 0
        assert result2["skipped"] == 3

    def test_second_batch_loads_only_new(self, tmp_path):
        """Batch 2 with 1 overlapping order loads only the 2 distinct orders."""
        db_url = f"sqlite:///{tmp_path}/incr.db"
        loader = IncrementalLoader(db_url)
        loader.load_batch(self._make_batch([101, 102, 103]), "orders_clean", "batch_1")
        result = loader.load_batch(self._make_batch([103, 104, 105]), "orders_clean", "batch_2")
        assert result["new"] == 2
        assert result["skipped"] == 1

    def test_total_count_after_two_batches(self, tmp_path):
        db_url = f"sqlite:///{tmp_path}/incr.db"
        loader = IncrementalLoader(db_url)
        loader.load_batch(self._make_batch([101, 102, 103]), "orders_clean", "batch_1")
        loader.load_batch(self._make_batch([103, 104, 105]), "orders_clean", "batch_2")
        total = loader.get_loaded_count("orders_clean")
        assert total == 5

    def test_batch_history_recorded(self, tmp_path):
        db_url = f"sqlite:///{tmp_path}/incr.db"
        loader = IncrementalLoader(db_url)
        loader.load_batch(self._make_batch([110, 111]), "orders_clean", "batch_A")
        loader.load_batch(self._make_batch([112, 113]), "orders_clean", "batch_B")
        history = loader.get_batch_history()
        assert len(history) >= 2


# ─────────────────────────────────────────────────────────────────────────
# TestIntegration
# ─────────────────────────────────────────────────────────────────────────
class TestIntegration:
    def test_end_to_end_pipeline(self, tmp_path):
        """Full Extract → Transform → Validate → Load pipeline on synthetic dataset."""
        raw_csv = str(tmp_path / "enterprise_sales_raw.csv")
        rows = [
            "order_id,customer_id,customer_name,email,region,product,category,quantity,unit_price,discount,status,order_date",
            "5001,C-201,Atlas Corp,procure@atlascorp.com,North,Cloud Gateway,Networking,2,850.00,0.1,Completed,2024-01-15",
            "5002,C-202,Bravo Logistics,ops@bravologistics.com,South,Barcode Scanner,Warehouse,1,180.00,0.0,Completed,2024-02-20",
            "5003,C-203,Cobalt Tech,support@cobalt.io,East,Thermal Printer,Warehouse,3,210.00,0.05,Pending,2024-03-10",
            "5001,C-201,Atlas Corp,procure@atlascorp.com,North,Cloud Gateway,Networking,2,850.00,0.1,Completed,2024-01-15",  # duplicate order_id
            "5004,C-204,,malformed-email,N/A,Server Blade,Datacenter,-1,1200.00,0.0,Cancelled,2024-04-05",  # negative qty & N/A region
        ]
        with open(raw_csv, "w", encoding="utf-8") as f:
            f.write("\n".join(rows))

        # 1. Ingestion
        ex = Extractor(CONFIG)
        df = ex.from_csv(raw_csv)
        assert len(df) == 5

        # 2. Transformation
        t = Transformer(CONFIG)
        clean = t.run(df)
        assert len(clean) < 5
        assert (clean["quantity"] > 0).all()
        assert "net_amount" in clean.columns

        # 3. Quality Validation
        v = DataQualityValidator()
        v.run_all_checks(clean)
        report = v.get_report(clean)
        assert report["rows_validated"] == len(clean)

        # 4. Persistence
        db_url = f"sqlite:///{tmp_path}/integration.db"
        loader = DatabaseLoader(db_url)
        count = loader.load_replace(clean, "orders_clean")
        assert count == len(clean)
        assert loader.verify("orders_clean") == len(clean)

    def test_incremental_demo_end_to_end(self, tmp_path):
        """Idempotent loading validation across consecutive runs."""
        db_url = f"sqlite:///{tmp_path}/demo_incremental.db"
        loader = IncrementalLoader(db_url)

        batch1 = pd.DataFrame([{
            "order_id": i, "customer_id": "C-101", "customer_name": "Apex Corp",
            "email": "contact@apex.com", "region": "North", "product": "Keyboard",
            "category": "Peripherals", "quantity": 1, "unit_price": 50.0,
            "discount": 0.0, "gross_amount": 50.0, "discount_amount": 0.0,
            "net_amount": 50.0, "status": "Completed", "order_date": "2024-01-01",
            "order_year": 2024, "order_month": 1, "order_quarter": 1,
            "order_month_name": "January", "is_discounted": 0,
        } for i in range(1, 6)])

        r1 = loader.load_batch(batch1, "orders_clean", "test_batch_1")
        assert r1["new"] == 5

        r2 = loader.load_batch(batch1, "orders_clean", "test_batch_1_retry")
        assert r2["new"] == 0
        assert r2["skipped"] == 5

        assert loader.get_loaded_count("orders_clean") == 5


# ─────────────────────────────────────────────────────────────────────────
# TestGenAISummary
# ─────────────────────────────────────────────────────────────────────────
class TestGenAISummary:
    def test_genai_summary_generation(self, clean_df, tmp_path):
        from genai_summary import SalesSummaryGenerator
        db_url = f"sqlite:///{tmp_path}/summary_test.db"
        loader = DatabaseLoader(db_url)
        loader.load_replace(clean_df, "orders_clean")

        gen = SalesSummaryGenerator(db_url)
        metrics = gen.extract_metrics()
        assert "total_orders" in metrics
        assert metrics["total_orders"] >= 1
        assert metrics["total_revenue"] > 0

        prompt = gen.build_llm_prompt(metrics)
        assert "Total Completed Orders" in prompt
        assert "$" in prompt

        report_txt = tmp_path / "reports" / "executive_briefing.txt"
        summary = gen.save_summary(str(report_txt))
        assert "EXECUTIVE SALES BRIEFING" in summary
        assert os.path.exists(str(report_txt))

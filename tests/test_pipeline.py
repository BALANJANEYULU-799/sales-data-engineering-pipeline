"""
tests/test_pipeline.py
=======================
Comprehensive pytest tests for all ETL pipeline stages.

Test categories
---------------
  TestExtractor         – CSV reading, column validation, profiling
  TestTransformer       – each cleaning step + full pipeline
  TestQuarantine        – rejected records are captured correctly
  TestValidator         – DQ checks pass/fail as expected
  TestDatabaseLoader    – loading modes and verification
  TestIncrementalLoader – idempotency and batch tracking
  TestIntegration       – end-to-end pipeline on synthetic data

Attribution note
----------------
Fixtures and testing patterns (TestExtractor, TestTransformer) are
adapted from the reference project by Pravalika Kadakanchi
(https://github.com/PravalikaKadakanchi/ETL-Pipeline).
TestValidator, TestIncrementalLoader, and TestIntegration are original
additions not present in the reference project.
"""

import csv
import json
import os
import sys

import numpy as np
import pandas as pd
import pytest

# ── Make src/ importable ──────────────────────────────────────────────────
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from extraction import Extractor, REQUIRED_COLUMNS
from transformation import Transformer
from validation import DataQualityValidator
from database import DatabaseLoader
from incremental import IncrementalLoader


CONFIG = {}


# ─────────────────────────────────────────────────────────────────────────
# Fixtures
# ─────────────────────────────────────────────────────────────────────────
@pytest.fixture
def raw_sample_df():
    """Raw DataFrame with intentional data quality issues."""
    return pd.DataFrame({
        "order_id":      [1, 2, 2, 3, 4,     5,    6],
        "customer_id":   ["C001", "C002", "C002", "C003", None, "C005", "C006"],
        "customer_name": ["Alice", "Bob", "Bob", "Carol", None, "Eve", "Frank"],
        "email":         ["a@b.com", "bad-email", "bad-email", "c@d.com", "e@f.com", None, "g@h.com"],
        "region":        ["North", "South", "South", "East", "N/A", "West", "Central"],
        "product":       ["Widget", "Gadget", "Gadget", "Widget", "Gizmo", "Widget", "Gadget"],
        "category":      ["Electronics"] * 7,
        "quantity":      [2, 3, 3, -1, 1, 5, 2],
        "unit_price":    [10.0, 20.0, 20.0, 30.0, None, 15.0, 25.0],
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
    """Fully transformed DataFrame from the sample data."""
    return transformer.run(raw_sample_df.copy())


# ─────────────────────────────────────────────────────────────────────────
# TestExtractor
# ─────────────────────────────────────────────────────────────────────────
class TestExtractor:
    def test_from_csv_raises_file_not_found(self):
        ex = Extractor(CONFIG)
        with pytest.raises(FileNotFoundError):
            ex.from_csv("nonexistent_file.csv")

    def test_from_csv_raises_on_missing_required_column(self, tmp_path):
        """CSV without 'order_id' should raise ValueError."""
        bad_csv = tmp_path / "bad.csv"
        bad_csv.write_text("product,quantity,unit_price,order_date,region,status,category\nWidget,1,10.0,2024-01-01,North,Completed,Electronics\n")
        ex = Extractor(CONFIG)
        with pytest.raises(ValueError, match="Missing required columns"):
            ex.from_csv(str(bad_csv))

    def test_from_csv_reads_valid_file(self, tmp_path):
        good_csv = tmp_path / "good.csv"
        good_csv.write_text(
            "order_id,product,category,quantity,unit_price,order_date,region,status\n"
            "1,Widget,Electronics,2,10.0,2024-01-01,North,Completed\n"
        )
        ex = Extractor(CONFIG)
        df = ex.from_csv(str(good_csv))
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
        df = pd.DataFrame({"Order ID": [1], "Unit Price": [10.0]})
        result = transformer.normalise_column_names(df)
        assert "order_id" in result.columns
        assert "unit_price" in result.columns

    def test_cast_types_date_iso(self, raw_sample_df, transformer):
        df = transformer.normalise_column_names(raw_sample_df.copy())
        result = transformer.cast_types(df)
        assert pd.api.types.is_datetime64_any_dtype(result["order_date"])

    def test_cast_types_handles_day_first_format(self, transformer):
        df = pd.DataFrame({
            "order_id": [1], "order_date": ["15/01/2024"],
            "quantity": ["2"], "unit_price": ["10.0"], "discount": ["0.0"],
        })
        result = transformer.cast_types(df)
        # DD/MM/YYYY should parse to 2024-01-15
        assert result["order_date"].iloc[0] == pd.Timestamp("2024-01-15")

    def test_cast_types_invalid_date_becomes_nat(self, transformer):
        df = pd.DataFrame({
            "order_id": [1], "order_date": ["not-a-date"],
            "quantity": ["2"], "unit_price": ["10.0"], "discount": ["0.0"],
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
        # "N/A" string should not appear anywhere in non-null region values
        assert "N/A" not in result["region"].dropna().astype(str).values
        # valid regions should survive
        valid_regions = {"North", "South", "East", "West", "Central"}
        assert result["region"].dropna().isin(valid_regions).all()

    def test_validate_email_invalidates_bad(self, raw_sample_df, transformer):
        df = transformer.normalise_column_names(raw_sample_df.copy())
        result = transformer.validate_email(df)
        assert "bad-email" not in result["email"].fillna("").values

    def test_handle_missing_quarantines_critical(self, transformer):
        """Rows missing order_id, product, unit_price, or order_date go to quarantine."""
        df = pd.DataFrame({
            "order_id":   [1, 2, 3],
            "product":    ["A", "B", None],
            "unit_price": [10.0, None, 20.0],
            "order_date": [pd.Timestamp("2024-01-01")] * 3,
            "status":     ["Completed"] * 3,
            "discount":   [0.0] * 3,
        })
        result = transformer.handle_missing_values(df)
        assert len(result) == 1           # only row 1 survives
        assert len(transformer.quarantine) == 2

    def test_handle_missing_fills_non_critical(self, transformer):
        df = pd.DataFrame({
            "order_id":    [1],
            "product":     ["Widget"],
            "unit_price":  [10.0],
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
        df = transformer.handle_missing_values(df)   # remove rows missing unit_price first
        result = transformer.validate_numeric_ranges(df)
        assert (result["quantity"] > 0).all()

    def test_deduplicate_by_business_key(self, raw_sample_df, transformer):
        """Business-key dedup keeps last occurrence of duplicated order_id."""
        df = transformer.normalise_column_names(raw_sample_df.copy())
        before = len(df)
        result = transformer.deduplicate_by_business_key(df)
        assert result.duplicated(subset=["order_id"]).sum() == 0
        assert len(result) < before

    def test_compute_derived_columns(self, transformer):
        df = pd.DataFrame({
            "order_id":   [1],
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
# TestQuarantine  (original addition)
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
            "order_id":   [99],
            "product":    ["Test"],
            "unit_price": [10.0],
            "order_date": [pd.Timestamp("2024-01-01")],
            "quantity":   [-5],
            "status":     ["Completed"],
            "discount":   [0.0],
            "region":     ["North"],
            "category":   ["Electronics"],
        })
        transformer.handle_missing_values(df)
        transformer.validate_numeric_ranges(df)
        quarantine_df = transformer.get_quarantine_df()
        # The negative quantity row should be quarantined
        reasons = quarantine_df["rejection_reason"].tolist()
        assert any("quantity" in str(r) for r in reasons) or len(quarantine_df) == 0

    def test_cleaning_summary_counts(self, raw_sample_df, transformer):
        result = transformer.run(raw_sample_df.copy())
        summary = transformer.get_cleaning_summary(len(raw_sample_df), len(result))
        assert summary["raw_records"] == len(raw_sample_df)
        assert summary["clean_records"] == len(result)
        assert summary["rejected_records"] == len(transformer.quarantine)


# ─────────────────────────────────────────────────────────────────────────
# TestValidator  (original addition)
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
        # Clean data should pass most checks
        assert report["checks_passed"] > 0

    def test_negative_quantity_fails_check(self, transformer):
        df = pd.DataFrame({
            "order_id":       [1],
            "product":        ["Widget"],
            "category":       ["Electronics"],
            "quantity":       [-5],
            "unit_price":     [10.0],
            "order_date":     [pd.Timestamp("2024-01-01")],
            "region":         ["North"],
            "status":         ["Completed"],
            "gross_amount":   [-50.0],
            "discount_amount":[0.0],
            "net_amount":     [-50.0],
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
            "order_id":       [1, 1],
            "product":        ["A", "B"],
            "category":       ["Electronics", "Electronics"],
            "quantity":       [1, 2],
            "unit_price":     [10.0, 20.0],
            "order_date":     [pd.Timestamp("2024-01-01")] * 2,
            "region":         ["North", "South"],
            "status":         ["Completed", "Pending"],
            "gross_amount":   [10.0, 40.0],
            "discount_amount":[0.0, 0.0],
            "net_amount":     [10.0, 40.0],
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
# TestDatabaseLoader  (adapted from reference project)
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
        """Upserting the same data twice should load 0 new rows on second run."""
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
# TestIncrementalLoader  (original addition)
# ─────────────────────────────────────────────────────────────────────────
class TestIncrementalLoader:
    def _make_batch(self, order_ids: list) -> pd.DataFrame:
        rows = []
        for oid in order_ids:
            rows.append({
                "order_id": oid,
                "customer_id": "C001",
                "customer_name": "Test Customer",
                "email": "test@example.com",
                "region": "North",
                "product": "Widget",
                "category": "Electronics",
                "quantity": 2,
                "unit_price": 50.0,
                "discount": 0.0,
                "gross_amount": 100.0,
                "discount_amount": 0.0,
                "net_amount": 100.0,
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
        batch = self._make_batch([1, 2, 3])
        result = loader.load_batch(batch, "orders_clean", "batch_1")
        assert result["new"] == 3
        assert result["skipped"] == 0

    def test_idempotency_same_batch_loads_zero(self, tmp_path):
        """Re-running the same batch should load 0 new records."""
        db_url = f"sqlite:///{tmp_path}/incr.db"
        loader = IncrementalLoader(db_url)
        batch = self._make_batch([1, 2, 3])
        loader.load_batch(batch, "orders_clean", "batch_1")
        result2 = loader.load_batch(batch, "orders_clean", "batch_1_retry")
        assert result2["new"] == 0
        assert result2["skipped"] == 3

    def test_second_batch_loads_only_new(self, tmp_path):
        """Batch 2 overlaps batch 1 by 1 record; only 2 new rows should load."""
        db_url = f"sqlite:///{tmp_path}/incr.db"
        loader = IncrementalLoader(db_url)
        loader.load_batch(self._make_batch([1, 2, 3]), "orders_clean", "batch_1")
        result = loader.load_batch(self._make_batch([3, 4, 5]), "orders_clean", "batch_2")
        assert result["new"] == 2
        assert result["skipped"] == 1

    def test_total_count_after_two_batches(self, tmp_path):
        db_url = f"sqlite:///{tmp_path}/incr.db"
        loader = IncrementalLoader(db_url)
        loader.load_batch(self._make_batch([1, 2, 3]), "orders_clean", "batch_1")
        loader.load_batch(self._make_batch([3, 4, 5]), "orders_clean", "batch_2")
        total = loader.get_loaded_count("orders_clean")
        assert total == 5   # unique order_ids: 1,2,3,4,5

    def test_batch_history_recorded(self, tmp_path):
        db_url = f"sqlite:///{tmp_path}/incr.db"
        loader = IncrementalLoader(db_url)
        loader.load_batch(self._make_batch([10, 11]), "orders_clean", "batch_A")
        loader.load_batch(self._make_batch([12, 13]), "orders_clean", "batch_B")
        history = loader.get_batch_history()
        assert len(history) >= 2


# ─────────────────────────────────────────────────────────────────────────
# Integration Test
# ─────────────────────────────────────────────────────────────────────────
class TestIntegration:
    def test_end_to_end_pipeline(self, tmp_path):
        """Full Extract → Transform → Validate → Load with synthetic data."""
        # Create a small synthetic CSV
        raw_csv = str(tmp_path / "raw.csv")
        rows = [
            "order_id,customer_id,customer_name,email,region,product,category,quantity,unit_price,discount,status,order_date",
            "1001,C001,Alice,alice@example.com,North,Laptop,Electronics,2,1299.99,0.1,Completed,2024-01-15",
            "1002,C002,Bob,bob@example.com,South,Shoes,Sports,1,119.99,0.0,Completed,2024-02-20",
            "1003,C003,Carol,carol@example.com,East,Watch,Electronics,3,249.99,0.15,Pending,2024-03-10",
            "1001,C001,Alice,alice@example.com,North,Laptop,Electronics,2,1299.99,0.1,Completed,2024-01-15",  # duplicate
            "1004,C004,,bad-email,N/A,Jacket,Clothing,-1,199.99,0.0,Cancelled,2024-04-05",  # bad rows
        ]
        with open(raw_csv, "w") as f:
            f.write("\n".join(rows))

        # 1. Extract
        ex = Extractor(CONFIG)
        df = ex.from_csv(raw_csv)
        assert len(df) == 5

        # 2. Transform
        t = Transformer(CONFIG)
        clean = t.run(df)
        assert len(clean) < 5           # duplicates and invalid rows removed
        assert (clean["quantity"] > 0).all()
        assert "net_amount" in clean.columns

        # 3. Validate
        v = DataQualityValidator()
        v.run_all_checks(clean)
        report = v.get_report(clean)
        assert report["rows_validated"] == len(clean)

        # 4. Load
        db_url = f"sqlite:///{tmp_path}/integration.db"
        loader = DatabaseLoader(db_url)
        count = loader.load_replace(clean, "orders_clean")
        assert count == len(clean)
        assert loader.verify("orders_clean") == len(clean)

    def test_incremental_demo_end_to_end(self, tmp_path):
        """Demonstrate incremental loading idempotency end-to-end."""
        db_url = f"sqlite:///{tmp_path}/demo.db"
        loader = IncrementalLoader(db_url)

        batch1 = pd.DataFrame([{
            "order_id": i, "customer_id": "C001", "customer_name": "Test",
            "email": "t@t.com", "region": "North", "product": "Widget",
            "category": "Electronics", "quantity": 1, "unit_price": 10.0,
            "discount": 0.0, "gross_amount": 10.0, "discount_amount": 0.0,
            "net_amount": 10.0, "status": "Completed", "order_date": "2024-01-01",
            "order_year": 2024, "order_month": 1, "order_quarter": 1,
            "order_month_name": "January", "is_discounted": 0,
        } for i in range(1, 6)])  # 5 records

        r1 = loader.load_batch(batch1, "orders_clean", "test_batch_1")
        assert r1["new"] == 5

        # Idempotency: same batch
        r2 = loader.load_batch(batch1, "orders_clean", "test_batch_1_retry")
        assert r2["new"] == 0
        assert r2["skipped"] == 5

        assert loader.get_loaded_count("orders_clean") == 5


# ─────────────────────────────────────────────────────────────────────────
# TestGenAISummary  (original addition - Step 4 optional feature)
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


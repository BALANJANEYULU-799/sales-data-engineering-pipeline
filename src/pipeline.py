"""
src/pipeline.py
===============
Main ETL orchestrator: ties together Extract → Profile → Transform →
Validate → Load → Summarise and writes reports.

Usage
-----
    python src/pipeline.py               # full pipeline
    python src/pipeline.py --mode upsert # incremental / idempotent
"""

import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime

import pandas as pd

# Make sure src/ is importable when run from the project root
sys.path.insert(0, os.path.join(os.path.dirname(__file__)))

from extraction import Extractor, REQUIRED_COLUMNS
from profiling import profile_dataframe, save_profile, print_profile_summary
from transformation import Transformer
from validation import DataQualityValidator
from database import DatabaseLoader
from incremental import IncrementalLoader

# ── Logging ────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

# ── Config ─────────────────────────────────────────────────────────────────
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

CONFIG = {
    "raw_data_path":       os.path.join(BASE_DIR, "data", "raw", "sales_raw.csv"),
    "processed_data_path": os.path.join(BASE_DIR, "data", "processed", "sales_clean.csv"),
    "quarantine_path":     os.path.join(BASE_DIR, "data", "quarantine", "rejected_records.csv"),
    "db_url":              f"sqlite:///{os.path.join(BASE_DIR, 'data', 'sales_pipeline.db')}",
    "profile_report":      os.path.join(BASE_DIR, "reports", "data_profile.json"),
    "dq_report":           os.path.join(BASE_DIR, "reports", "data_quality_report.json"),
    "pipeline_report":     os.path.join(BASE_DIR, "reports", "pipeline_report.json"),
    "genai_report":        os.path.join(BASE_DIR, "reports", "executive_briefing.txt"),
    "main_table":          "orders_clean",
}


def run_pipeline(mode: str = "replace", batch_label: str = "full_load") -> dict:
    """
    Execute the complete ETL pipeline.

    Parameters
    ----------
    mode : 'replace' (full reload) or 'upsert' (incremental / idempotent)
    batch_label : label used for incremental tracking
    """
    start_time = time.time()

    print("\n" + "=" * 65)
    print("  SALES DATA ENGINEERING PIPELINE")
    print(f"  Mode: {mode.upper()}   Batch: {batch_label}")
    print(f"  Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 65)

    # ── STAGE 1: EXTRACT ──────────────────────────────────────────────
    print("\n[1/6] EXTRACT")
    extractor = Extractor(CONFIG)
    raw_df = extractor.from_csv(CONFIG["raw_data_path"])
    print(f"      ✓  Read {len(raw_df):,} rows from {CONFIG['raw_data_path']}")

    # ── STAGE 2: PROFILE ──────────────────────────────────────────────
    print("\n[2/6] PROFILE")
    raw_profile = profile_dataframe(raw_df, label="raw_sales")
    os.makedirs(os.path.dirname(CONFIG["profile_report"]), exist_ok=True)
    save_profile(raw_profile, CONFIG["profile_report"])
    print(f"      ✓  Profile saved → {CONFIG['profile_report']}")
    print(f"         Rows: {raw_profile['shape']['rows']:,}  |  "
          f"Duplicates: {raw_profile['duplicate_rows']:,}  |  "
          f"Total nulls: {raw_profile['total_nulls']:,}")

    # ── STAGE 3: TRANSFORM ────────────────────────────────────────────
    print("\n[3/6] TRANSFORM")
    transformer = Transformer(CONFIG)
    clean_df = transformer.run(raw_df)
    quarantine_df = transformer.get_quarantine_df()
    cleaning_summary = transformer.get_cleaning_summary(len(raw_df), len(clean_df))

    # Save clean CSV
    os.makedirs(os.path.dirname(CONFIG["processed_data_path"]), exist_ok=True)
    clean_df.to_csv(CONFIG["processed_data_path"], index=False)

    # Save quarantine CSV
    if not quarantine_df.empty:
        os.makedirs(os.path.dirname(CONFIG["quarantine_path"]), exist_ok=True)
        quarantine_df.to_csv(CONFIG["quarantine_path"], index=False)

    print(f"      ✓  Clean records : {len(clean_df):,}")
    print(f"      ✓  Rejected/quarantined: {len(quarantine_df):,}")

    # ── STAGE 4: VALIDATE ─────────────────────────────────────────────
    print("\n[4/6] VALIDATE")
    validator = DataQualityValidator()
    validator.run_all_checks(clean_df)
    dq_report = validator.get_report(clean_df)

    os.makedirs(os.path.dirname(CONFIG["dq_report"]), exist_ok=True)
    with open(CONFIG["dq_report"], "w", encoding="utf-8") as f:
        json.dump(dq_report, f, indent=2, default=str)

    print(f"      ✓  Checks: {dq_report['checks_passed']}/{dq_report['total_checks']} passed  "
          f"[{dq_report['overall_status']}]")
    print(f"      ✓  DQ report saved → {CONFIG['dq_report']}")

    # ── STAGE 5: LOAD ─────────────────────────────────────────────────
    print("\n[5/6] LOAD")
    if mode == "upsert":
        inc_loader = IncrementalLoader(CONFIG["db_url"])
        load_result = inc_loader.load_batch(clean_df, CONFIG["main_table"], batch_label)
        rows_loaded = load_result["new"]
        rows_skipped = load_result["skipped"]
        print(f"      ✓  Incremental load: {rows_loaded:,} new | {rows_skipped:,} skipped")
    else:
        db_loader = DatabaseLoader(CONFIG["db_url"])
        rows_loaded = db_loader.load_replace(clean_df, CONFIG["main_table"])
        rows_skipped = 0
        print(f"      ✓  Full replace: {rows_loaded:,} rows loaded")

    # Build summary tables
    db_loader_for_summaries = DatabaseLoader(CONFIG["db_url"])
    db_loader_for_summaries.build_summaries(clean_df)
    print("      ✓  Summary tables built")

    # ── STAGE 6: REPORT ───────────────────────────────────────────────
    print("\n[6/6] PIPELINE REPORT")
    elapsed = round(time.time() - start_time, 2)

    report = {
        "pipeline_version": "1.0.0",
        "run_at": datetime.now().isoformat(),
        "mode": mode,
        "batch_label": batch_label,
        "elapsed_seconds": elapsed,
        "extract": {
            "source": CONFIG["raw_data_path"],
            "rows_read": raw_profile["shape"]["rows"],
            "duplicates_detected": raw_profile["duplicate_rows"],
            "nulls_detected": raw_profile["total_nulls"],
        },
        "transform": cleaning_summary,
        "validate": {
            "checks_run": dq_report["total_checks"],
            "checks_passed": dq_report["checks_passed"],
            "checks_failed": dq_report["checks_failed"],
            "overall_status": dq_report["overall_status"],
        },
        "load": {
            "db_url": CONFIG["db_url"],
            "table": CONFIG["main_table"],
            "mode": mode,
            "rows_loaded": rows_loaded,
            "rows_skipped": rows_skipped,
        },
        "data_quality": {
            "total_revenue": round(float(clean_df["net_amount"].sum()), 2),
            "avg_order_value": round(float(clean_df["net_amount"].mean()), 2),
            "orders_with_discount": int(clean_df["is_discounted"].sum()),
            "status_breakdown": clean_df["status"].value_counts().to_dict(),
            "region_breakdown": clean_df["region"].value_counts().to_dict(),
        },
    }

    os.makedirs(os.path.dirname(CONFIG["pipeline_report"]), exist_ok=True)
    with open(CONFIG["pipeline_report"], "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, default=str)

    # Optional Step 4: GenAI Executive Briefing Report
    try:
        from genai_summary import SalesSummaryGenerator
        gen = SalesSummaryGenerator(CONFIG["db_url"])
        gen.save_summary(CONFIG["genai_report"])
    except Exception as e:
        logger.warning("Could not generate executive briefing report: %s", e)

    # Record metadata in DB
    try:
        meta_loader = DatabaseLoader(CONFIG["db_url"])
        meta_loader.record_pipeline_run({
            "run_at": report["run_at"],
            "mode": mode,
            "rows_input": raw_profile["shape"]["rows"],
            "rows_loaded": rows_loaded,
            "rows_rejected": len(quarantine_df),
            "elapsed_sec": elapsed,
            "status": dq_report["overall_status"],
        })
    except Exception as e:
        logger.warning("Could not record pipeline run metadata: %s", e)

    print(f"\n{'=' * 65}")
    print("  PIPELINE COMPLETE")
    print(f"  Rows extracted : {report['extract']['rows_read']:,}")
    print(f"  Rows clean     : {len(clean_df):,}")
    print(f"  Rows rejected  : {len(quarantine_df):,}")
    print(f"  Rows loaded    : {rows_loaded:,}")
    print(f"  DQ status      : {dq_report['overall_status']}")
    print(f"  Total revenue  : ${report['data_quality']['total_revenue']:,.2f}")
    print(f"  Elapsed        : {elapsed}s")
    print(f"  Reports        : {os.path.dirname(CONFIG['pipeline_report'])}/")
    print("=" * 65)

    return report


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Sales Data Engineering Pipeline")
    parser.add_argument(
        "--mode",
        choices=["replace", "upsert"],
        default="replace",
        help="Loading mode: replace (full reload) or upsert (incremental)",
    )
    parser.add_argument(
        "--batch",
        default="full_load",
        help="Batch label for incremental tracking (default: full_load)",
    )
    args = parser.parse_args()
    run_pipeline(mode=args.mode, batch_label=args.batch)

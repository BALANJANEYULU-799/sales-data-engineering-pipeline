"""
src/cli.py
==========
Interactive CLI for the Sales Data Engineering Pipeline.

Commands
--------
  run       – Run the full ETL pipeline
  profile   – Profile the raw CSV (no DB write)
  validate  – Run data-quality checks on the last clean dataset
  analytics – Execute all analytics queries against the DB
  summary   – Show the last pipeline run report
  demo-incr – Demonstrate incremental loading with two batches
  help      – Show this help message
  quit      – Exit

Run:  python src/cli.py
"""

import json
import os
import sys

if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass

_proj_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_lib_dir = os.path.join(_proj_root, "lib")
if os.path.isdir(_lib_dir) and _lib_dir not in sys.path:
    sys.path.insert(0, _lib_dir)
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import pandas as pd

from extraction import Extractor
from profiling import profile_dataframe, save_profile, print_profile_summary
from transformation import Transformer
from validation import DataQualityValidator
from database import DatabaseLoader
from incremental import IncrementalLoader
from analytics import AnalyticsRunner
import pipeline as _pipeline

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CONFIG = _pipeline.CONFIG


def _banner():
    print("\n" + "=" * 55)
    print("  Sales Data Engineering Pipeline — Interactive CLI")
    print("=" * 55)
    print("  Commands: run | profile | validate | analytics |")
    print("            summary | genai | demo-incr | help | quit")
    print("=" * 55)


def _cmd_run():
    print("\nChoose loading mode:")
    print("  1) replace  — full reload (drops existing data)")
    print("  2) upsert   — incremental (skips already-loaded records)")
    choice = input("  Enter 1 or 2 [default 1]: ").strip() or "1"
    mode = "upsert" if choice == "2" else "replace"
    batch = input("  Batch label [default: full_load]: ").strip() or "full_load"
    _pipeline.run_pipeline(mode=mode, batch_label=batch)


def _cmd_profile():
    raw_path = CONFIG["raw_data_path"]
    if not os.path.exists(raw_path):
        print(f"\n  ✗ Raw data not found: {raw_path}")
        print("  Run the sample-data generator first:")
        print("    python data/sample/generate_sample_data.py")
        return
    extractor = Extractor(CONFIG)
    df = extractor.from_csv(raw_path)
    profile = profile_dataframe(df, "raw_sales")
    save_profile(profile, CONFIG["profile_report"])
    print_profile_summary(profile)
    print(f"\n  Profile saved → {CONFIG['profile_report']}")


def _cmd_validate():
    processed_path = CONFIG["processed_data_path"]
    if not os.path.exists(processed_path):
        print("\n  ✗ No clean data found. Run the pipeline first (command: run).")
        return
    df = pd.read_csv(processed_path)
    if "order_date" in df.columns:
        df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce")
    validator = DataQualityValidator()
    validator.run_all_checks(df)
    report = validator.get_report(df)

    print(f"\n  DQ Status: {report['overall_status']}")
    print(f"  Checks: {report['checks_passed']}/{report['total_checks']} passed")
    print("\n  Failed checks:")
    failed = [c for c in report["checks"] if not c["passed"]]
    if failed:
        for c in failed:
            print(f"    ✗ {c['check_name']}: {c['failures']} failures")
    else:
        print("    ✓ All checks passed!")

    with open(CONFIG["dq_report"], "w") as f:
        json.dump(report, f, indent=2, default=str)
    print(f"\n  DQ report saved → {CONFIG['dq_report']}")


def _cmd_analytics():
    db_path = CONFIG["db_url"].replace("sqlite:///", "")
    if not os.path.exists(db_path):
        print("\n  ✗ Database not found. Run the pipeline first (command: run).")
        return
    runner = AnalyticsRunner(CONFIG["db_url"])
    print("\n  Available queries:")
    for i, name in enumerate(runner.list_queries(), 1):
        print(f"    {i:>2}. {name}")
    choice = input("\n  Enter query name or 'all' [default: all]: ").strip() or "all"
    if choice == "all":
        runner.run_all()
    elif choice in runner.list_queries():
        runner.run(choice)
    else:
        print(f"  Unknown query: {choice}")


def _cmd_summary():
    report_path = CONFIG["pipeline_report"]
    if not os.path.exists(report_path):
        print("\n  ✗ No pipeline report found. Run the pipeline first.")
        return
    with open(report_path) as f:
        report = json.load(f)
    print("\n" + "=" * 55)
    print("  LAST PIPELINE RUN SUMMARY")
    print("=" * 55)
    print(f"  Run at        : {report['run_at']}")
    print(f"  Mode          : {report['mode']}")
    print(f"  Elapsed       : {report['elapsed_seconds']}s")
    print(f"  Rows extracted: {report['extract']['rows_read']:,}")
    print(f"  Rows clean    : {report['transform']['clean_records']:,}")
    print(f"  Rows rejected : {report['transform']['rejected_records']:,}")
    print(f"  Rows loaded   : {report['load']['rows_loaded']:,}")
    print(f"  DQ status     : {report['validate']['overall_status']}")
    print(f"  Total revenue : ${report['data_quality']['total_revenue']:,.2f}")
    print("=" * 55)


def _cmd_genai():
    """Generates an executive sales briefing using deterministic SQL metrics."""
    db_path = CONFIG["db_url"].replace("sqlite:///", "")
    if not os.path.exists(db_path):
        print("\n  ✗ Database not found. Run the pipeline first (command: run).")
        return
    from genai_summary import SalesSummaryGenerator
    gen = SalesSummaryGenerator(CONFIG["db_url"])
    output_path = os.path.join(BASE_DIR, "reports", "executive_briefing.txt")
    print(gen.save_summary(output_path))
    print(f"\n  Briefing saved → {output_path}")


def _cmd_demo_incr():
    """
    Demonstrates incremental loading with two synthetic batches.
    Shows that re-running the same batch loads 0 new rows (idempotency).
    """
    print("\n" + "=" * 55)
    print("  INCREMENTAL LOADING DEMONSTRATION")
    print("=" * 55)

    demo_db = f"sqlite:///{os.path.join(BASE_DIR, 'data', 'demo_incremental.db')}"
    os.makedirs(os.path.join(BASE_DIR, "data"), exist_ok=True)

    inc = IncrementalLoader(demo_db)

    def _make_batch(order_ids, start_row=1):
        rows = []
        for i, oid in enumerate(order_ids):
            rows.append({
                "order_id": oid,
                "customer_id": f"C{i+1:03d}",
                "customer_name": f"Customer {i+1}",
                "email": f"customer{i+1}@example.com",
                "region": ["North", "South", "East", "West", "Central"][i % 5],
                "product": ["Laptop", "Headphones", "Watch", "Shoes", "Jacket"][i % 5],
                "category": ["Electronics", "Electronics", "Electronics", "Sports", "Clothing"][i % 5],
                "quantity": (i % 5) + 1,
                "unit_price": [1299.99, 89.99, 249.99, 119.99, 199.99][i % 5],
                "discount": 0.0,
                "gross_amount": [1299.99, 89.99, 249.99, 119.99, 199.99][i % 5] * ((i % 5) + 1),
                "discount_amount": 0.0,
                "net_amount": [1299.99, 89.99, 249.99, 119.99, 199.99][i % 5] * ((i % 5) + 1),
                "status": "Completed",
                "order_date": f"2024-0{(i%9)+1}-15",
                "order_year": 2024,
                "order_month": (i % 9) + 1,
                "order_quarter": ((i % 9) // 3) + 1,
                "order_month_name": ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep"][i % 9],
                "is_discounted": 0,
            })
        return pd.DataFrame(rows)

    batch1 = _make_batch(list(range(1001, 1006)))
    batch2_new = _make_batch(list(range(1006, 1011)))

    print("\n  Step 1: Load Batch 1 (order_ids 1001–1005)")
    r1 = inc.load_batch(batch1, "orders_clean", "batch_001")
    print(f"         → New: {r1['new']}  Skipped: {r1['skipped']}")

    print("\n  Step 2: Reload Batch 1 (idempotency test — should load 0 new rows)")
    r1b = inc.load_batch(batch1, "orders_clean", "batch_001_retry")
    print(f"         → New: {r1b['new']}  Skipped: {r1b['skipped']}")
    if r1b["new"] == 0:
        print("         ✓ Idempotency confirmed! Re-running same batch loads 0 rows.")

    print("\n  Step 3: Load Batch 2 (order_ids 1006–1010 — all new)")
    r2 = inc.load_batch(batch2_new, "orders_clean", "batch_002")
    print(f"         → New: {r2['new']}  Skipped: {r2['skipped']}")

    total = inc.get_loaded_count("orders_clean")
    print(f"\n  Total rows in demo DB: {total}")

    history = inc.get_batch_history()
    print("\n  Batch history:")
    print(history.to_string(index=False))
    print("=" * 55)


COMMANDS = {
    "run": _cmd_run,
    "profile": _cmd_profile,
    "validate": _cmd_validate,
    "analytics": _cmd_analytics,
    "summary": _cmd_summary,
    "genai": _cmd_genai,
    "demo-incr": _cmd_demo_incr,
}


def main():
    _banner()
    while True:
        try:
            cmd = input("\n  > ").strip().lower()
        except (KeyboardInterrupt, EOFError):
            print("\n  Goodbye!")
            break

        if cmd in ("quit", "exit", "q"):
            print("  Goodbye!")
            break
        elif cmd in ("help", ""):
            _banner()
        elif cmd in COMMANDS:
            try:
                COMMANDS[cmd]()
            except Exception as e:
                print(f"\n  ✗ Error: {e}")
        else:
            print(f"  Unknown command: '{cmd}'. Type 'help' to see commands.")


if __name__ == "__main__":
    main()

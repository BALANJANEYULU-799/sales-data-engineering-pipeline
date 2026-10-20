"""
data/sample/generate_sample_data.py
====================================
Generates a small, reproducible sample sales dataset for the pipeline demo.

Usage
-----
    python data/sample/generate_sample_data.py

Output
------
    data/raw/sales_raw.csv        — 200 rows with intentional data issues
    data/sample/batch_001.csv     — rows 1–100 (for incremental demo batch 1)
    data/sample/batch_002.csv     — rows 101–150 new + 50 from batch 1 (overlap)

Design
------
• random.seed(42) ensures reproducibility — every run produces the same data.
• Data issues intentionally injected: duplicates, missing values,
  invalid statuses, negative quantities, mixed date formats.
• All issues are documented in INJECTION_LOG at the bottom of this file.
"""

import csv
import os
import random
import sys
from datetime import datetime, timedelta

# Ensure UTF-8 output encoding on Windows consoles
if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass

random.seed(42)

# ── Reference data ──────────────────────────────────────────────────────────
REGIONS = ["North", "South", "East", "West", "Central"]
STATUSES = ["Completed", "Pending", "Cancelled", "Refunded",
            "COMPLETED", "pending", None]          # mixed case + None → cleaning demo

PRODUCTS = [
    ("Laptop Pro 15",       "Electronics",        1299.99),
    ("Wireless Headphones", "Electronics",           89.99),
    ("USB-C Hub",           "Electronics",           49.99),
    ("Running Shoes",       "Sports",              119.99),
    ("Yoga Mat",            "Sports",               34.99),
    ("Winter Jacket",       "Clothing",            199.99),
    ("T-Shirt Pack",        "Clothing",             29.99),
    ("Coffee Maker",        "Home & Garden",        79.99),
    ("Garden Hose",         "Home & Garden",        44.99),
    ("Organic Coffee",      "Food & Beverage",      18.99),
    ("Protein Powder",      "Food & Beverage",      54.99),
    ("Smart Watch",         "Electronics",         249.99),
]

CUSTOMERS = [
    (f"CUST{str(i).zfill(4)}",
     random.choice(["Alice", "Bob", "Carol", "David", "Eve",
                    "Frank", "Grace", "Henry", "Iris", "Jack"])
     + " "
     + random.choice(["Smith", "Jones", "Brown", "Davis", "Wilson"]),
     f"user{i}@{'gmail' if i % 3 == 0 else 'yahoo' if i % 3 == 1 else 'outlook'}.com",
     random.choice(REGIONS))
    for i in range(1, 51)
]


FIXED_BASE_DATE = datetime(2026, 10, 21)


def random_date(start_days_ago: int = 365, end_days_ago: int = 0) -> datetime:
    start = FIXED_BASE_DATE - timedelta(days=start_days_ago)
    end = FIXED_BASE_DATE - timedelta(days=end_days_ago)
    return start + (end - start) * random.random()


def generate_orders(n: int = 200) -> list[dict]:
    rows = []
    order_id = 10001

    for _ in range(n):
        cust = random.choice(CUSTOMERS)
        prod_name, category, unit_price = random.choice(PRODUCTS)
        qty = random.randint(1, 10)
        discount = random.choice([0, 0, 0, 0.05, 0.10, 0.15, 0.20])
        date = random_date()

        # ── Intentional data quality issues (documented) ─────────────────
        status = random.choice(STATUSES)                            # mixed case / None
        price = unit_price if random.random() > 0.05 else None      # 5% missing price
        cust_id = cust[0] if random.random() > 0.03 else None       # 3% missing customer_id
        email = (
            cust[2] if random.random() > 0.06
            else ("bad-email" if random.random() > 0.5 else None)   # 6% bad/missing email
        )
        date_str = (
            date.strftime("%Y-%m-%d") if random.random() > 0.05
            else date.strftime("%d/%m/%Y")                          # 5% DD/MM/YYYY format
        )
        qty_injected = qty if random.random() > 0.03 else -qty      # 3% negative quantity

        rows.append({
            "order_id":      order_id,
            "customer_id":   cust_id,
            "customer_name": cust[1] if random.random() > 0.02 else None,
            "email":         email,
            "region":        (
                cust[3] if random.random() > 0.04
                else random.choice(["", "N/A", cust[3]])            # 4% blank/N/A region
            ),
            "product":       prod_name,
            "category":      category,
            "quantity":      qty_injected,
            "unit_price":    price,
            "discount":      discount,
            "status":        status,
            "order_date":    date_str,
        })
        order_id += 1

    # Inject ~5% duplicate rows
    dupes = random.sample(rows, int(n * 0.05))
    rows.extend(dupes)
    random.shuffle(rows)
    return rows


def save_csv(rows: list[dict], path: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
    print(f"  Saved {len(rows):,} rows -> {path}")


if __name__ == "__main__":
    # Determine project root (2 levels up from this script)
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(os.path.dirname(script_dir))

    print("Generating sample sales data …")

    # Full dataset (main input for the pipeline)
    all_rows = generate_orders(200)
    save_csv(all_rows, os.path.join(project_root, "data", "raw", "sales_raw.csv"))

    # Batch 1: first 100 unique order_ids (for incremental demo)
    # We extract unique rows from all_rows by order_id
    seen_ids = set()
    unique_rows = []
    for r in all_rows:
        if r["order_id"] not in seen_ids:
            seen_ids.add(r["order_id"])
            unique_rows.append(r)

    batch1 = unique_rows[:100]
    batch2 = unique_rows[80:150]  # 20 overlap with batch1, 50 new — shows incremental behaviour

    save_csv(batch1, os.path.join(project_root, "data", "sample", "batch_001.csv"))
    save_csv(batch2, os.path.join(project_root, "data", "sample", "batch_002.csv"))

    print("\nData generation complete.")
    print("  data/raw/sales_raw.csv        -> full pipeline input")
    print("  data/sample/batch_001.csv     -> incremental demo batch 1")
    print("  data/sample/batch_002.csv     -> incremental demo batch 2 (overlapping)")

    # ── INJECTION LOG ──────────────────────────────────────────────────────
    print("\nInjected data quality issues:")
    print("  - ~5% duplicate rows (random.sample duplication)")
    print("  - ~5% missing unit_price values")
    print("  - ~3% missing customer_id values")
    print("  - ~6% missing or malformed email addresses")
    print("  - ~5% dates in DD/MM/YYYY format (vs standard YYYY-MM-DD)")
    print("  - ~3% negative quantity values")
    print("  - ~4% blank or 'N/A' region values")
    print("  - Mixed-case status values (e.g., 'COMPLETED', 'pending')")

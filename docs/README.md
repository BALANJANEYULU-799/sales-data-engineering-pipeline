# Documentation Index

Welcome to the internal documentation for the **Sales Data Engineering Pipeline**.

This repository is organized to provide an easily navigable, clean codebase for software engineers and data practitioners.

---

## Architecture Overview

The pipeline implements an end-to-end batch ETL workflow:

```
[Raw CSV Batches] ──> [Extraction & Schema Contract] ──> [Data Profiling]
                             │
                             ▼
               [Stage-Driven Transformation]
                ├── Valid Records ──> [Pre-Load DQ Gate (13 Checks)] ──> [Database Loader]
                │                                                              │
                └── Corrupted ──────> [Quarantine CSV Store]                   ├──> [orders_clean (Flat 3NF)]
                                                                               ├──> [Star Schema (fact & dims)]
                                                                               └──> [pipeline_runs (Audit)]
```

---

## Module Reference

| Directory / File | Description |
|---|---|
| [`src/extraction.py`](../src/extraction.py) | Ingests CSV files, enforces schema contracts against `REQUIRED_COLUMNS`, and manages multi-batch source tracking. |
| [`src/profiling.py`](../src/profiling.py) | Generates statistical JSON summaries of data shapes, null percentages, and numeric distributions. |
| [`src/transformation.py`](../src/transformation.py) | Implements an 8-phase transformation pipeline: type casting, status/region cleaning, business-key deduplication, and derived metrics. |
| [`src/validation.py`](../src/validation.py) | Executes 13 automated pre-load data quality assertions and produces a PASS/FAIL JSON scorecard. |
| [`src/database.py`](../src/database.py) | Manages SQLAlchemy SQLite connections, full table replaces, star schema generation, and audit lineage logging. |
| [`src/incremental.py`](../src/incremental.py) | Implements watermark-based idempotent delta loading via `loaded_order_ids`. |
| [`src/analytics.py`](../src/analytics.py) | Programmatic runner for 12 advanced SQL analytics queries. |
| [`src/pipeline.py`](../src/pipeline.py) | Master orchestrator tying all stages together. |
| [`src/cli.py`](../src/cli.py) | Interactive command-line interface with 7 operational subcommands. |
| [`src/genai_summary.py`](../src/genai_summary.py) | Aggregates database KPIs into structured LLM prompt templates and deterministic briefings. |
| [`sql/schema.sql`](../sql/schema.sql) | DDL statements defining tables, indexes, and star schema relationships. |
| [`sql/analytics.sql`](../sql/analytics.sql) | 12 SQL business intelligence queries (CTEs, window functions, and dimensional joins). |

---

## Data Model & Schemas

### Operational Store
- **`orders_clean`**: 3NF table indexed on `status`, `category`, `region`, `order_date`, and `(order_year, order_month)`.

### Kimball Dimensional Model
- **`fact_sales`**: Granular transaction fact table holding sales metrics (`quantity`, `unit_price`, `gross_amount`, `discount_amount`, `net_amount`).
- **`dim_product`**: Product catalog dimension (`product_id`, `product_name`, `category`).
- **`dim_date`**: Calendar date hierarchy (`date_key`, `full_date`, `year`, `month`, `month_name`, `quarter`, `day_of_week`).
- **`dim_region`**: Geographic territory dimension (`region_id`, `region_name`).

---

## Extended Learning & Interview Guides

In accordance with clean repository standards, exhaustive knowledge-transfer guides, interview prep questions, and audit logs are maintained in an external documentation archive outside the version-controlled application repository (`sales-data-engineering-documentation/`).

# Interview Preparation Notes
# Sales Data Engineering Pipeline

---

## Quick 60-Second Project Explanation

> "I built a Sales Data Engineering Pipeline using Python, pandas, SQL, and SQLite.
> The pipeline reads raw CSV sales data with intentional data quality issues — missing values,
> duplicates, mixed date formats, and negative quantities.
> It cleans and validates the data through a 10-step transformation pipeline,
> saves rejected records to a quarantine file with documented reasons,
> and loads the clean data into a SQLite database.
> I also implemented incremental loading, which means re-running the pipeline
> with the same batch loads zero new records — this property is called idempotency.
> The project includes 41 automated pytest tests, a GitHub Actions CI workflow,
> SQL analytics queries demonstrating window functions and CTEs, and an interactive CLI.
> I adapted the core ETL structure from an open-source reference project by Pravalika Kadakanchi
> and added several new features: a quarantine module, a named DQ validation module,
> incremental loading, a star schema demonstration, and extended tests."

---

## Core Concepts

### What does ETL mean?
**E**xtract, **T**ransform, **L**oad.
- **Extract**: read data from a source (CSV, database, API)
- **Transform**: clean, validate, and reshape the data
- **Load**: write the clean data to a destination (database, data warehouse)

In this project: CSV → pandas DataFrame → SQLite.

### Why Python and pandas?
- Python is the dominant language for data engineering
- pandas provides fast, readable DataFrame operations for tabular data
- Easy to write, test, and maintain compared to raw SQL or Java

### Why SQLite?
- Zero configuration — no server needed, just a file
- Built into Python's standard library
- Perfect for local development and demos
- Can be swapped for PostgreSQL by changing one line: the `db_url` string

### What is a DataFrame?
A table-like data structure with rows and columns, similar to an Excel spreadsheet.
pandas operations (filtering, grouping, merging) work on the entire column at once,
which is much faster than row-by-row Python loops.

---

## Data Cleaning

### How are duplicate records handled?
**Business-key deduplication by `order_id`.**
- When multiple rows share the same `order_id`, we keep the LAST occurrence
  (the most recently ingested version, which is likely the most up-to-date).
- This differs from generic row-level deduplication (which the reference project uses),
  because two rows can share an `order_id` but differ in status (e.g., Pending → Completed).

### How are rejected records managed?
Rejected records go to a **quarantine CSV** (`data/quarantine/rejected_records.csv`).
Each rejected row has a `rejection_reason` column explaining why it was removed.

Rejection rules (documented):
1. Missing `order_id`, `product`, `unit_price`, or `order_date` → quarantine
2. `quantity ≤ 0` → quarantine
3. `unit_price ≤ 0` → quarantine
4. Discount outside [0, 1] → soft correction (clamp), not quarantine

This is important: you should never silently delete data without documenting why.

### How are missing values handled?
- **Critical fields** (order_id, product, unit_price, order_date): rows missing these are quarantined
- **Non-critical fields**: filled with documented defaults
  - `status` → 'Pending'
  - `discount` → 0.0
  - `region` → 'Unknown'
  - `customer_name` → 'Anonymous'
  - `customer_id` → 'UNKNOWN'

---

## Database and Loading

### How does the database schema work?
Two models:
1. **Flat model** (`orders_clean`): one big table with all columns; easy to query
2. **Star schema** (`fact_sales` + `dim_product`, `dim_date`, `dim_region`):
   separates facts (measurements) from dimensions (descriptive attributes)

**Grain of `fact_sales`**: one row = one sales order. `order_id` is the primary key.

**Why star schema?**
- Reduces repeated text in the fact table
- Makes analytical queries (GROUP BY category, region, quarter) faster and cleaner
- Industry standard for data warehouses (Redshift, BigQuery, Snowflake)

### What are indexes and why do they matter?
An index is like a book's index — it lets the database find rows without reading every row.

Example: `CREATE INDEX idx_orders_status ON orders_clean(status);`
- Without index: query `WHERE status = 'Completed'` reads all 182 rows
- With index: the DB jumps directly to matching rows

---

## Incremental Loading

### How does incremental loading work?
1. A tracking table `loaded_order_ids` stores every `order_id` that was successfully loaded
2. When a new batch arrives, we compare its `order_id`s against the tracking table
3. Only NEW `order_id`s are inserted; existing ones are skipped
4. The tracking table is updated with the new IDs

### What is idempotency?
Running the same operation multiple times produces the same result.
- First run: loads 5 records
- Second run (same data): loads 0 records, skips 5 — **idempotent!**

This is critical in production pipelines where network failures can cause retries.

### What is the difference between replace, append, and upsert?
- **Replace**: drop and recreate the table; clean slate each run
- **Append**: add all new rows; can create duplicates if run twice
- **Upsert** (this project): insert only NEW records; skip existing ones — idempotent

---

## SQL Concepts

### What is GROUP BY?
Aggregates rows with the same value in one or more columns.
```sql
SELECT region, SUM(net_amount) FROM orders_clean GROUP BY region;
```

### What is HAVING?
Like WHERE but filters GROUPS (after GROUP BY), not individual rows.
```sql
GROUP BY category HAVING COUNT(*) > 5
```

### What is a CTE?
Common Table Expression — a named temporary result set using `WITH`.
Makes complex queries readable by breaking them into steps.

### What is a window function?
Computes a value across a set of rows related to the current row, without collapsing them.
```sql
SUM(revenue) OVER (ORDER BY month) -- running total
ROW_NUMBER() OVER (PARTITION BY region ORDER BY revenue DESC) -- rank within region
```

### What is the difference between INNER JOIN and LEFT JOIN?
- **INNER JOIN**: only returns rows that match in BOTH tables
- **LEFT JOIN**: returns ALL rows from the left table; NULL for non-matching right rows

---

## Testing

### How does pytest verify correctness?
Each test function checks one specific behaviour:
- **Positive tests**: verify the expected happy-path behaviour
- **Negative tests**: verify that errors or edge cases are handled correctly

Example negative test:
```python
def test_from_csv_raises_file_not_found():
    ex = Extractor({})
    with pytest.raises(FileNotFoundError):
        ex.from_csv("nonexistent_file.csv")
```

### What is a pytest fixture?
A reusable setup function that creates test data or objects.
The `@pytest.fixture` decorator makes it available to any test that lists it as a parameter.

---

## Likely Interview Questions

### Q1: What is an ETL pipeline?
**A:** ETL stands for Extract, Transform, Load. It's a process to move data from a source, clean and reshape it, and load it into a destination like a database or data warehouse. In my project, I extract from CSV, transform using pandas, and load into SQLite.

### Q2: What is data profiling?
**A:** Analyzing a dataset's structure and quality before processing — counting nulls, finding duplicates, checking data types, and computing basic statistics. My `profiling.py` generates a JSON report with all of this.

### Q3: What is data quality validation?
**A:** Running named checks to verify that data meets business rules after cleaning. My validator runs 13 checks: no missing order_ids, positive quantities and prices, unique business keys, etc.

### Q4: What is a quarantine in data engineering?
**A:** Instead of silently deleting bad records, we save them separately with a reason explaining why they were rejected. This makes the pipeline auditable and allows humans to review and fix bad data.

### Q5: What is idempotency and why does it matter?
**A:** An operation is idempotent if running it multiple times gives the same result. In my incremental loading, re-running the same batch loads 0 new records. This matters because network failures can cause pipelines to retry, and you don't want duplicate data.

### Q6: How did you handle duplicate records?
**A:** I used business-key deduplication on `order_id`. When duplicates exist, I keep the last occurrence (most recently ingested). I also document this rule explicitly in the code so anyone reading it understands why.

### Q7: Why SQLite instead of MySQL or PostgreSQL?
**A:** SQLite requires no server or configuration, making it ideal for local demos and development. The code uses SQLAlchemy, so changing one line (`sqlite:///` → `postgresql://`) would migrate to any production database.

### Q8: What is a star schema?
**A:** A dimensional data model with one central fact table connected to dimension tables. The fact table stores measurable events (orders); dimensions store descriptive attributes (products, dates, regions). It makes analytical queries faster and easier.

### Q9: What is the grain of your fact table?
**A:** One row per sales order. `order_id` is the primary key, so each order appears exactly once.

### Q10: What is a window function?
**A:** A SQL function that computes a value across a set of related rows without collapsing them. For example, `ROW_NUMBER() OVER (PARTITION BY region ORDER BY revenue DESC)` ranks products within each region.

### Q11: What is a CTE and why use it?
**A:** Common Table Expression. A named temporary result set using `WITH`. It makes complex queries readable by naming intermediate steps. My `monthly_running_total` query uses a CTE to compute monthly revenue before adding a running total.

### Q12: What is PySpark and when would you use it?
**A:** PySpark is the Python API for Apache Spark — a distributed data processing framework. You'd use it when data is too large for a single machine (millions of rows). My project uses pandas, which is appropriate for smaller datasets.

### Q13: What is an index in a database?
**A:** A data structure that speeds up query lookups. Like a book's index, it lets the database jump directly to matching rows instead of scanning every row. I created indexes on `status`, `category`, `region`, and `order_date`.

### Q14: What is the difference between REPLACE, APPEND, and UPSERT loading?
**A:** REPLACE drops and recreates the table each run. APPEND adds all rows and can create duplicates. UPSERT inserts only new records and skips existing ones — it's idempotent.

### Q15: What is GitHub Actions CI?
**A:** Continuous Integration — automatically running tests on every code push. My `tests.yml` workflow runs all 41 pytest tests on Python 3.10, 3.11, and 3.12 whenever I push to GitHub.

### Q16: What did you implement versus what came from the reference project?
**A:** I adapted the core ETL class structure and transformer test fixtures from the reference project by Pravalika Kadakanchi. I added: quarantine module, standalone DQ validation module, incremental loading, star schema, interactive CLI, pipeline_runs metadata table, and 18 additional tests. I document this clearly in the README's attribution section.

### Q17: How would you scale this to production?
**A:** Replace SQLite with PostgreSQL or BigQuery, add Apache Airflow for scheduling, use S3 or GCS for raw file storage, containerize with Docker, and add monitoring with logging dashboards.

### Q18: What is SQLAlchemy?
**A:** A Python library that provides a database abstraction layer. Instead of writing database-specific SQL, you use the same Python code for SQLite, PostgreSQL, MySQL, etc. Only the connection URL changes.

### Q19: What is a HAVING clause?
**A:** It filters groups after GROUP BY, similar to how WHERE filters rows. `HAVING COUNT(*) > 5` shows only groups with more than 5 records.

### Q20: How do you ensure your pipeline is reproducible?
**A:** The sample data generator uses `random.seed(42)`, so every run produces identical data. Tests use pytest's `tmp_path` fixture for isolated temporary directories. The clean state is always achievable by deleting the database file.

---

## What I Learned from This Project

1. How real ETL pipelines handle dirty data
2. The importance of documenting cleaning rules rather than silently dropping data
3. Why idempotency matters in production pipelines
4. How star schemas differ from flat tables and when to use each
5. How to write parameterized database queries safely (SQL injection prevention)
6. How GitHub Actions CI catches bugs automatically
7. The difference between unit tests (testing one function) and integration tests (testing the full pipeline)

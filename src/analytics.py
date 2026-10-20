"""
src/analytics.py
================
SQL analytics runner: executes predefined queries against the SQLite DB
and prints formatted results to the terminal.

This is an original addition to the reference project.  The reference project
provides a SQL file and a separate generate_report.py script.  This module
provides a Python-callable analytics engine that can be invoked by the CLI.
"""

import logging

import pandas as pd
from sqlalchemy import create_engine, text

logger = logging.getLogger(__name__)


# ------------------------------------------------------------------ #
# Named queries (comments explain each SQL concept for learning)
# ------------------------------------------------------------------ #
QUERIES = {
    "overall_kpis": {
        "title": "Overall KPIs",
        "description": "Total revenue, orders, and average order value for completed orders.",
        "sql": """
            -- SELECT, WHERE, aggregate functions (COUNT, SUM, AVG, ROUND)
            SELECT
                COUNT(*)                        AS total_orders,
                ROUND(SUM(net_amount), 2)       AS total_revenue,
                ROUND(AVG(net_amount), 2)       AS avg_order_value,
                ROUND(SUM(discount_amount), 2)  AS total_discounts,
                COUNT(DISTINCT customer_id)     AS unique_customers
            FROM orders_clean
            WHERE status = 'Completed'
        """,
    },
    "monthly_trend": {
        "title": "Monthly Revenue Trend",
        "description": "Month-by-month revenue (GROUP BY, ORDER BY).",
        "sql": """
            -- GROUP BY, ORDER BY, aggregate functions
            SELECT
                order_year,
                order_month,
                order_month_name,
                COUNT(*)                   AS total_orders,
                ROUND(SUM(net_amount), 2)  AS monthly_revenue,
                ROUND(AVG(net_amount), 2)  AS avg_order_value
            FROM orders_clean
            WHERE status = 'Completed'
            GROUP BY order_year, order_month, order_month_name
            ORDER BY order_year, order_month
        """,
    },
    "revenue_by_category": {
        "title": "Revenue by Category",
        "description": "Category performance (GROUP BY, HAVING example inline).",
        "sql": """
            -- GROUP BY with HAVING to show only categories with > 5 orders
            SELECT
                category,
                COUNT(*)                    AS total_orders,
                ROUND(SUM(net_amount), 2)   AS total_revenue,
                SUM(quantity)               AS units_sold
            FROM orders_clean
            WHERE status = 'Completed'
            GROUP BY category
            HAVING COUNT(*) > 5
            ORDER BY total_revenue DESC
        """,
    },
    "top_products": {
        "title": "Top 10 Products by Revenue",
        "description": "Best-selling products (LIMIT, ORDER BY DESC).",
        "sql": """
            -- LIMIT, ORDER BY DESC
            SELECT
                product,
                category,
                COUNT(*)                   AS orders,
                SUM(quantity)              AS units_sold,
                ROUND(SUM(net_amount), 2)  AS total_revenue
            FROM orders_clean
            WHERE status = 'Completed'
            GROUP BY product, category
            ORDER BY total_revenue DESC
            LIMIT 10
        """,
    },
    "revenue_by_region": {
        "title": "Revenue by Region",
        "description": "Regional breakdown (GROUP BY, aggregate functions).",
        "sql": """
            SELECT
                region,
                COUNT(*)                   AS total_orders,
                ROUND(SUM(net_amount), 2)  AS total_revenue,
                ROUND(AVG(net_amount), 2)  AS avg_order_value
            FROM orders_clean
            WHERE status = 'Completed'
            GROUP BY region
            ORDER BY total_revenue DESC
        """,
    },
    "status_breakdown": {
        "title": "Order Status Breakdown",
        "description": "Distribution of order statuses (CASE for labelling).",
        "sql": """
            -- CASE statement to add a human-readable label
            SELECT
                status,
                COUNT(*) AS order_count,
                ROUND(SUM(net_amount), 2) AS total_value,
                CASE
                    WHEN COUNT(*) * 100.0 / SUM(COUNT(*)) OVER () >= 50 THEN 'Majority'
                    WHEN COUNT(*) * 100.0 / SUM(COUNT(*)) OVER () >= 20 THEN 'Significant'
                    ELSE 'Minority'
                END AS volume_label
            FROM orders_clean
            GROUP BY status
            ORDER BY order_count DESC
        """,
    },
    "top_customers": {
        "title": "Top 10 Customers by Lifetime Value",
        "description": "Customer segmentation using aggregate + subquery concepts.",
        "sql": """
            -- Subquery approach: wrap a grouped result to add a rank column
            SELECT
                customer_id,
                customer_name,
                COUNT(*)                   AS total_orders,
                ROUND(SUM(net_amount), 2)  AS lifetime_value,
                ROUND(AVG(net_amount), 2)  AS avg_order_value,
                MIN(order_date)            AS first_order,
                MAX(order_date)            AS last_order
            FROM orders_clean
            WHERE status = 'Completed'
              AND customer_id IS NOT NULL
              AND customer_id != 'UNKNOWN'
            GROUP BY customer_id, customer_name
            ORDER BY lifetime_value DESC
            LIMIT 10
        """,
    },
    "discount_effectiveness": {
        "title": "Discount Effectiveness by Tier",
        "description": "CASE statement to classify discount tiers.",
        "sql": """
            -- CASE statement for conditional bucketing
            SELECT
                CASE
                    WHEN discount = 0      THEN 'No Discount'
                    WHEN discount <= 0.05  THEN '1-5%'
                    WHEN discount <= 0.10  THEN '6-10%'
                    WHEN discount <= 0.15  THEN '11-15%'
                    ELSE '16%+'
                END                            AS discount_tier,
                COUNT(*)                       AS total_orders,
                ROUND(AVG(net_amount), 2)      AS avg_order_value,
                ROUND(SUM(net_amount), 2)      AS total_revenue
            FROM orders_clean
            WHERE status = 'Completed'
            GROUP BY discount_tier
            ORDER BY total_orders DESC
        """,
    },
    "rank_products_by_region": {
        "title": "Product Rank within Each Region",
        "description": "ROW_NUMBER window function to rank products per region.",
        "sql": """
            -- Window function: ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ...)
            SELECT
                region,
                product,
                ROUND(SUM(net_amount), 2) AS revenue,
                ROW_NUMBER() OVER (
                    PARTITION BY region ORDER BY SUM(net_amount) DESC
                ) AS rank_in_region
            FROM orders_clean
            WHERE status = 'Completed'
            GROUP BY region, product
            ORDER BY region, rank_in_region
        """,
    },
    "monthly_running_total": {
        "title": "Monthly Running Revenue Total",
        "description": "Running total using SUM() window function (CTE pattern).",
        "sql": """
            -- CTE (WITH clause) + running SUM window function
            WITH monthly AS (
                SELECT
                    order_year,
                    order_month,
                    ROUND(SUM(net_amount), 2) AS monthly_revenue
                FROM orders_clean
                WHERE status = 'Completed'
                GROUP BY order_year, order_month
            )
            SELECT
                order_year,
                order_month,
                monthly_revenue,
                ROUND(SUM(monthly_revenue) OVER (
                    ORDER BY order_year, order_month
                ), 2) AS running_total
            FROM monthly
            ORDER BY order_year, order_month
        """,
    },
    "dq_audit": {
        "title": "Data Quality Audit Query",
        "description": "Check for remaining data issues in the loaded table.",
        "sql": """
            -- Data quality validation queries
            SELECT
                COUNT(*)                                         AS total_rows,
                SUM(CASE WHEN order_id IS NULL THEN 1 ELSE 0 END)   AS null_order_id,
                SUM(CASE WHEN product IS NULL THEN 1 ELSE 0 END)    AS null_product,
                SUM(CASE WHEN unit_price IS NULL THEN 1 ELSE 0 END) AS null_price,
                SUM(CASE WHEN quantity <= 0 THEN 1 ELSE 0 END)      AS invalid_qty,
                SUM(CASE WHEN net_amount < 0 THEN 1 ELSE 0 END)     AS negative_revenue
            FROM orders_clean
        """,
    },
}


class AnalyticsRunner:
    """Executes named SQL analytics queries and displays results."""

    def __init__(self, db_url: str):
        self.db_url = db_url
        self.engine = create_engine(db_url)

    def run(self, query_name: str, max_rows: int = 20) -> pd.DataFrame | None:
        """Execute a named query and return results as a DataFrame."""
        if query_name not in QUERIES:
            logger.error("Unknown query: %s. Available: %s", query_name, list(QUERIES.keys()))
            return None

        q = QUERIES[query_name]
        print(f"\n{'=' * 60}")
        print(f"  {q['title']}")
        print(f"  {q['description']}")
        print("=" * 60)

        with self.engine.connect() as conn:
            df = pd.read_sql(text(q["sql"]), conn)

        if df.empty:
            print("  (No results — check that the pipeline has been run.)")
            return df

        print(df.head(max_rows).to_string(index=False))
        if len(df) > max_rows:
            print(f"  … ({len(df) - max_rows} more rows)")
        return df

    def run_all(self, max_rows: int = 15) -> dict:
        """Run all named queries and return a dict of DataFrames."""
        results = {}
        for name in QUERIES:
            results[name] = self.run(name, max_rows)
        return results

    def list_queries(self) -> list[str]:
        return list(QUERIES.keys())

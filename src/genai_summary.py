"""
src/genai_summary.py
====================
GenAI-Ready Sales Summary & Prompt Generator.

Extracts deterministic KPI aggregates from the SQLite database, formats them
into a structured prompt context suitable for LLM ingestion, and produces a
deterministic executive briefing report. Operates without requiring paid API
keys or cloud credentials.

This component demonstrates how GenAI-enabled data pipelines prepare verified
analytical facts and structured prompts for downstream AI agents or LLMs.
It is a prompt/summary generator rather than a live LLM API integration.
"""

import json
import logging
import os
import sqlite3
from datetime import datetime

import pandas as pd
from sqlalchemy import create_engine, text

logger = logging.getLogger(__name__)


class SalesSummaryGenerator:
    """Generates an executive narrative summary from database analytics."""

    def __init__(self, db_url: str):
        self.db_url = db_url
        self.engine = create_engine(db_url)

    def extract_metrics(self) -> dict:
        """Query key deterministic metrics from the database."""
        with self.engine.connect() as conn:
            kpi_df = pd.read_sql(text("""
                SELECT
                    COUNT(*) AS total_orders,
                    ROUND(SUM(net_amount), 2) AS total_revenue,
                    ROUND(AVG(net_amount), 2) AS avg_order_value,
                    ROUND(SUM(discount_amount), 2) AS total_discounts
                FROM orders_clean
                WHERE status = 'Completed'
            """), conn)

            top_cat_df = pd.read_sql(text("""
                SELECT category, ROUND(SUM(net_amount), 2) AS revenue
                FROM orders_clean
                WHERE status = 'Completed'
                GROUP BY category
                ORDER BY revenue DESC
                LIMIT 1
            """), conn)

            top_reg_df = pd.read_sql(text("""
                SELECT region, ROUND(SUM(net_amount), 2) AS revenue
                FROM orders_clean
                WHERE status = 'Completed'
                GROUP BY region
                ORDER BY revenue DESC
                LIMIT 1
            """), conn)

            top_prod_df = pd.read_sql(text("""
                SELECT product, ROUND(SUM(net_amount), 2) AS revenue, SUM(quantity) AS units
                FROM orders_clean
                WHERE status = 'Completed'
                GROUP BY product
                ORDER BY revenue DESC
                LIMIT 1
            """), conn)

        return {
            "total_orders": int(kpi_df["total_orders"].iloc[0]) if not kpi_df.empty else 0,
            "total_revenue": float(kpi_df["total_revenue"].iloc[0]) if not kpi_df.empty else 0.0,
            "avg_order_value": float(kpi_df["avg_order_value"].iloc[0]) if not kpi_df.empty else 0.0,
            "total_discounts": float(kpi_df["total_discounts"].iloc[0]) if not kpi_df.empty else 0.0,
            "top_category": top_cat_df["category"].iloc[0] if not top_cat_df.empty else "N/A",
            "top_category_revenue": float(top_cat_df["revenue"].iloc[0]) if not top_cat_df.empty else 0.0,
            "top_region": top_reg_df["region"].iloc[0] if not top_reg_df.empty else "N/A",
            "top_region_revenue": float(top_reg_df["revenue"].iloc[0]) if not top_reg_df.empty else 0.0,
            "top_product": top_prod_df["product"].iloc[0] if not top_prod_df.empty else "N/A",
            "top_product_units": int(top_prod_df["units"].iloc[0]) if not top_prod_df.empty else 0,
        }

    def build_llm_prompt(self, metrics: dict) -> str:
        """Construct the prompt template that would be dispatched to an LLM."""
        return (
            "You are a Senior Business Data Analyst. Summarize the following verified sales pipeline metrics "
            "into an executive briefing report with key insights and recommended business actions:\n\n"
            f"- Total Completed Orders: {metrics['total_orders']:,}\n"
            f"- Gross Realized Revenue: ${metrics['total_revenue']:,.2f}\n"
            f"- Average Order Value (AOV): ${metrics['avg_order_value']:,.2f}\n"
            f"- Cumulative Discounts Given: ${metrics['total_discounts']:,.2f}\n"
            f"- Leading Category: {metrics['top_category']} (${metrics['top_category_revenue']:,.2f})\n"
            f"- Top Geographical Region: {metrics['top_region']} (${metrics['top_region_revenue']:,.2f})\n"
            f"- Highest Revenue Product: {metrics['top_product']} ({metrics['top_product_units']} units sold)\n"
        )

    def generate_narrative_summary(self, metrics: dict | None = None) -> str:
        """
        Generate a structured executive report based on deterministic metrics.
        No external API key is required.
        """
        if metrics is None:
            metrics = self.extract_metrics()

        discount_rate = (
            round(metrics["total_discounts"] / (metrics["total_revenue"] + metrics["total_discounts"]) * 100, 2)
            if (metrics["total_revenue"] + metrics["total_discounts"]) > 0 else 0.0
        )

        report = [
            "============================================================",
            "        EXECUTIVE SALES BRIEFING (GENAI ASSISTED)           ",
            "============================================================",
            f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            "",
            "1. FINANCIAL & OPERATIONAL HIGHLIGHTS",
            f"   - Total completed sales volume reached ${metrics['total_revenue']:,.2f} across {metrics['total_orders']:,} orders.",
            f"   - Average Order Value (AOV) stands at ${metrics['avg_order_value']:,.2f}.",
            f"   - Promotional discounts totaled ${metrics['total_discounts']:,.2f} (effective discount rate: {discount_rate}%).",
            "",
            "2. MARKET PERFORMANCE DRIVERS",
            f"   - Product Leader: '{metrics['top_product']}' drove top line revenue with {metrics['top_product_units']:,} units sold.",
            f"   - Category Dominance: '{metrics['top_category']}' generated ${metrics['top_category_revenue']:,.2f} in net sales.",
            f"   - Regional Stronghold: '{metrics['top_region']}' proved the most lucrative regional territory (${metrics['top_region_revenue']:,.2f}).",
            "",
            "3. STRATEGIC RECOMMENDATIONS",
            f"   - Optimize Inventory: Ensure stock resilience for '{metrics['top_product']}' to avoid stock-outs.",
            f"   - Regional Expansion: Replicate successful promotional playbooks from '{metrics['top_region']}' to lagging regions.",
            f"   - Margin Protection: Monitor discount rates ({discount_rate}%) to protect gross margins while testing targeted bundles.",
            "============================================================",
        ]
        return "\n".join(report)

    def save_summary(self, output_path: str) -> str:
        """Generate and save the report to file."""
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        metrics = self.extract_metrics()
        prompt = self.build_llm_prompt(metrics)
        narrative = self.generate_narrative_summary(metrics)

        content = f"--- LLM INGESTION PROMPT ---\n{prompt}\n\n--- EXECUTIVE NARRATIVE REPORT ---\n{narrative}\n"
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(content)

        logger.info("[GENAI] Saved executive briefing -> %s", output_path)
        return narrative

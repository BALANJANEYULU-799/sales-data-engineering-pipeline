"""
src/profiling.py
================
Data profiling stage: generates a rich JSON report describing the
shape, quality, and distribution of raw sales data.

This module is an original addition to the reference project
(Pravalika Kadakanchi / ETL-Pipeline). The reference project
includes a basic profile dict inside the Extractor class. This module
provides a standalone, detailed profiling step with file output.
"""

import json
import logging
import os
from datetime import datetime

import pandas as pd

logger = logging.getLogger(__name__)


def profile_dataframe(df: pd.DataFrame, label: str = "dataset") -> dict:
    """
    Produce a comprehensive profile dictionary for a DataFrame.

    Parameters
    ----------
    df    : pandas DataFrame to profile
    label : human-readable name for this dataset (used in report header)

    Returns
    -------
    dict  : profile suitable for JSON serialisation
    """
    null_counts = df.isnull().sum()
    duplicate_count = int(df.duplicated().sum())

    # Per-column details
    columns_info = {}
    for col in df.columns:
        col_null = int(null_counts[col])
        col_data = {
            "dtype": str(df[col].dtype),
            "null_count": col_null,
            "null_pct": round(col_null / len(df) * 100, 2) if len(df) > 0 else 0.0,
            "unique_count": int(df[col].nunique(dropna=True)),
        }

        # Numeric summary
        if pd.api.types.is_numeric_dtype(df[col]):
            desc = df[col].describe()
            col_data["min"] = _safe_float(desc.get("min"))
            col_data["max"] = _safe_float(desc.get("max"))
            col_data["mean"] = _safe_float(desc.get("mean"))
            col_data["std"] = _safe_float(desc.get("std"))
            col_data["median"] = _safe_float(df[col].median())

        # Top values for categorical columns (≤20 distinct values)
        elif df[col].nunique(dropna=True) <= 20:
            top_vals = df[col].value_counts(dropna=False).head(10).to_dict()
            col_data["top_values"] = {str(k): int(v) for k, v in top_vals.items()}

        columns_info[col] = col_data

    report = {
        "label": label,
        "generated_at": datetime.now().isoformat(),
        "shape": {"rows": len(df), "columns": len(df.columns)},
        "duplicate_rows": duplicate_count,
        "total_nulls": int(null_counts.sum()),
        "columns": columns_info,
    }

    logger.info(
        "[PROFILE] %s | %d rows | %d cols | %d duplicates | %d total nulls",
        label,
        len(df),
        len(df.columns),
        duplicate_count,
        int(null_counts.sum()),
    )

    return report


def save_profile(profile: dict, output_path: str) -> None:
    """Serialise profile dict to a JSON file."""
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(profile, f, indent=2, default=str)
    logger.info("[PROFILE] Saved → %s", output_path)


def print_profile_summary(profile: dict) -> None:
    """Print a human-readable profile summary to stdout."""
    print("\n" + "=" * 60)
    print(f"  DATA PROFILE: {profile['label']}")
    print("=" * 60)
    print(f"  Rows      : {profile['shape']['rows']:,}")
    print(f"  Columns   : {profile['shape']['columns']}")
    print(f"  Duplicates: {profile['duplicate_rows']:,}")
    print(f"  Total nulls: {profile['total_nulls']:,}")
    print("\n  Per-column null counts:")
    for col, info in profile["columns"].items():
        if info["null_count"] > 0:
            print(f"    {col:<20} {info['null_count']:>5} nulls ({info['null_pct']:.1f}%)")
    print("=" * 60)


# ------------------------------------------------------------------ #
# Private helpers
# ------------------------------------------------------------------ #
def _safe_float(val) -> float | None:
    try:
        return round(float(val), 4) if val is not None and not pd.isna(val) else None
    except (TypeError, ValueError):
        return None

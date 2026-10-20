"""
src/extraction.py
=================
Data Ingestion Stage: Ingests raw sales transactional data from CSV sources,
validates schema contracts against mandatory column definitions, handles multi-batch
ingestion with source lineage tracking, and computes diagnostic profiling telemetry.
"""

import os
import logging
import pandas as pd

logger = logging.getLogger(__name__)

# Columns that must be present in every sales CSV
REQUIRED_COLUMNS = {
    "order_id",
    "product",
    "category",
    "quantity",
    "unit_price",
    "order_date",
    "region",
    "status",
}


class Extractor:
    """Reads raw sales data from CSV files and produces a data profile."""

    def __init__(self, config: dict):
        self.config = config

    # ------------------------------------------------------------------ #
    # Single CSV ingestion
    # ------------------------------------------------------------------ #
    def from_csv(self, path: str, **kwargs) -> pd.DataFrame:
        """
        Read a CSV file and return a DataFrame.

        Parameters
        ----------
        path : str
            Absolute or relative path to the CSV file.
        **kwargs
            Additional arguments forwarded to pd.read_csv.

        Raises
        ------
        FileNotFoundError  – if the file does not exist.
        ValueError         – if required columns are missing.
        """
        if not os.path.exists(path):
            raise FileNotFoundError(f"Source file not found: {path}")

        df = pd.read_csv(path, **kwargs)
        logger.info("[EXTRACT] CSV | %s | %d rows | %d cols", path, len(df), len(df.columns))

        # --- Column validation (original feature) ---
        missing_cols = REQUIRED_COLUMNS - set(df.columns.str.lower().str.strip())
        if missing_cols:
            raise ValueError(
                f"Missing required columns in {path}: {sorted(missing_cols)}"
            )

        logger.info("[EXTRACT] All required columns present.")
        return df

    # ------------------------------------------------------------------ #
    # Multi-file merge  (original feature — not in reference project)
    # ------------------------------------------------------------------ #
    def from_multiple_csvs(self, paths: list, **kwargs) -> pd.DataFrame:
        """
        Read and vertically concatenate several CSV files.
        Adds a 'source_file' column to track origin of each record.
        """
        frames = []
        for p in paths:
            df = self.from_csv(p, **kwargs)
            df["source_file"] = os.path.basename(p)
            frames.append(df)
        combined = pd.concat(frames, ignore_index=True)
        logger.info("[EXTRACT] Combined %d files → %d total rows", len(paths), len(combined))
        return combined

    # ------------------------------------------------------------------ #
    # Data profiling
    # ------------------------------------------------------------------ #
    def profile(self, df: pd.DataFrame) -> dict:
        """
        Generate a quick data-quality profile of a DataFrame.

        Returns a dictionary suitable for JSON serialisation.
        """
        null_counts = df.isnull().sum().to_dict()
        numeric_cols = df.select_dtypes(include="number").columns.tolist()

        profile = {
            "rows": len(df),
            "columns": list(df.columns),
            "column_count": len(df.columns),
            "dtypes": {c: str(t) for c, t in df.dtypes.items()},
            "null_counts": {k: int(v) for k, v in null_counts.items()},
            "null_pct": {
                c: round(v / len(df) * 100, 2) if len(df) > 0 else 0.0
                for c, v in null_counts.items()
            },
            "duplicates": int(df.duplicated().sum()),
            "numeric_stats": {},
        }

        # Basic descriptive statistics for numeric columns
        for col in numeric_cols:
            s = df[col].describe()
            profile["numeric_stats"][col] = {
                "min": round(float(s["min"]), 4) if not pd.isna(s["min"]) else None,
                "max": round(float(s["max"]), 4) if not pd.isna(s["max"]) else None,
                "mean": round(float(s["mean"]), 4) if not pd.isna(s["mean"]) else None,
                "std": round(float(s["std"]), 4) if not pd.isna(s["std"]) else None,
            }

        return profile

"""
Chapter 01: Hello Assets — CSV -> DuckDB

Three assets:
1) raw_iris: load CSV into a DataFrame
2) iris_clean: validate schema and drop nulls
3) iris_summary: aggregate by species and write to DuckDB
"""
from __future__ import annotations

import os
from pathlib import Path
import duckdb
import pandas as pd
from dagster import asset, Output, MetadataValue

def _paths():
    data_dir = Path(os.getenv("PTWR_DATA_DIR", Path(__file__).resolve().parents[4] / "data"))
    raw_csv = data_dir / "raw" / "iris.csv"
    warehouse_dir = data_dir / "warehouse"
    duckdb_path = warehouse_dir / "iris.duckdb"
    return data_dir, raw_csv, warehouse_dir, duckdb_path

@asset(name="raw_iris", group_name="iris", compute_kind="pandas", description="Load the Iris CSV from data/raw/iris.csv into a DataFrame.")
def raw_iris() -> Output[pd.DataFrame]:
    data_dir, raw_csv, *_ = _paths()
    raw_csv.parent.mkdir(parents=True, exist_ok=True)
    if not raw_csv.exists():
        raise FileNotFoundError(f"Expected CSV at {raw_csv}")
    df = pd.read_csv(raw_csv)
    preview = df.head(5).to_markdown(index=False)
    return Output(df, metadata={"rows": len(df), "columns": list(df.columns), "preview": MetadataValue.md(preview), "source_path": str(raw_csv)})

EXPECTED_COLUMNS = ["sepal_length", "sepal_width", "petal_length", "petal_width", "species"]

@asset(name="iris_clean", group_name="iris", compute_kind="pandas", description="Validate expected columns and drop rows with nulls.")
def iris_clean(raw_iris: pd.DataFrame) -> Output[pd.DataFrame]:
    missing = [c for c in EXPECTED_COLUMNS if c not in raw_iris.columns]
    if missing:
        raise ValueError(f"Missing columns: {missing}")
    numeric_cols = ["sepal_length", "sepal_width", "petal_length", "petal_width"]
    df = raw_iris.copy()
    for c in numeric_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    before = len(df)
    df = df.dropna(subset=numeric_cols + ["species"]).reset_index(drop=True)
    dropped = before - len(df)
    return Output(df, metadata={"rows_after": len(df), "rows_dropped": dropped})

@asset(name="iris_summary", group_name="iris", compute_kind="duckdb", description="Aggregate per-species stats and write to DuckDB at data/warehouse/iris.duckdb (table iris_summary).")
def iris_summary(iris_clean: pd.DataFrame) -> Output[pd.DataFrame]:
    summary = (iris_clean.groupby("species", as_index=False)
        .agg(count=("species", "count"), sepal_length_mean=("sepal_length", "mean"), sepal_width_mean=("sepal_width", "mean"), petal_length_mean=("petal_length", "mean"), petal_width_mean=("petal_width", "mean")))
    _, _, warehouse_dir, duckdb_path = _paths()
    warehouse_dir.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(duckdb_path))
    try:
        con.register("iris_summary_df", summary)
        con.execute("CREATE OR REPLACE TABLE iris_summary AS SELECT * FROM iris_summary_df")
    finally:
        con.close()
    return Output(summary, metadata={"duckdb_path": str(duckdb_path), "table": "iris_summary", "rows": len(summary), "preview": MetadataValue.md(summary.head().to_markdown(index=False))})

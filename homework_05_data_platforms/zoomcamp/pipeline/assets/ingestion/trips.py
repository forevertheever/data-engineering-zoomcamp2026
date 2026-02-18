"""@bruin

# TODO: Set the asset name (recommended pattern: schema.asset_name).
# - Convention in this module: use an `ingestion.` schema for raw ingestion tables.
name: ingestion.trips

# TODO: Set the asset type.
# Docs: https://getbruin.com/docs/bruin/assets/python
type: python

# TODO: Pick a Python image version (Bruin runs Python in isolated environments).
# Example: python:3.11
image: python:3.13

# TODO: Set the connection.
connection: duckdb-default

# TODO: Choose materialization (optional, but recommended).
# Bruin feature: Python materialization lets you return a DataFrame (or list[dict]) and Bruin loads it into your destination.
# This is usually the easiest way to build ingestion assets in Bruin.
# Alternative (advanced): you can skip Bruin Python materialization and write a "plain" Python asset that manually writes
# into DuckDB (or another destination) using your own client library and SQL. In that case:
# - you typically omit the `materialization:` block
# - you do NOT need a `materialize()` function; you just run Python code
# Docs: https://getbruin.com/docs/bruin/assets/python#materialization
materialization:
  # TODO: choose `table` or `view` (ingestion generally should be a table)
  type: table
  # TODO: pick a strategy.
  # suggested strategy: append
  strategy: append

# TODO: Define output columns (names + types) for metadata, lineage, and quality checks.
# Tip: mark stable identifiers as `primary_key: true` if you plan to use `merge` later.
# Docs: https://getbruin.com/docs/bruin/assets/columns
# columns:
#   - name: TODO_col1
#     type: TODO_type
#     description: TODO

@bruin"""

# TODO: Add imports needed for your ingestion (e.g., pandas, requests).
# - Put dependencies in the nearest `requirements.txt` (this template has one at the pipeline root).
# Docs: https://getbruin.com/docs/bruin/assets/python


# TODO: Only implement `materialize()` if you are using Bruin Python materialization.
# If you choose the manual-write approach (no `materialization:` block), remove this function and implement ingestion
# as a standard Python script instead.
# def materialize():
#     """
#     TODO: Implement ingestion using Bruin runtime context.

#     Required Bruin concepts to use here:
#     - Built-in date window variables:
#       - BRUIN_START_DATE / BRUIN_END_DATE (YYYY-MM-DD)
#       - BRUIN_START_DATETIME / BRUIN_END_DATETIME (ISO datetime)
#       Docs: https://getbruin.com/docs/bruin/assets/python#environment-variables
#     - Pipeline variables:
#       - Read JSON from BRUIN_VARS, e.g. `taxi_types`
#       Docs: https://getbruin.com/docs/bruin/getting-started/pipeline-variables

#     Design TODOs (keep logic minimal, focus on architecture):
#     - Use start/end dates + `taxi_types` to generate a list of source endpoints for the run window.
#     - Fetch data for each endpoint, parse into DataFrames, and concatenate.
#     - Add a column like `extracted_at` for lineage/debugging (timestamp of extraction).
#     - Prefer append-only in ingestion; handle duplicates in staging.
#     """
#     # return final_dataframe


"""
trips.py

Python ingestion asset for NYC Taxi trip parquet files.

Behavior (per zoomcamp/README.md):
- Downloads monthly parquet files from the TLC CDN:
  https://d37ci6vzurychx.cloudfront.net/trip-data/{taxi_type}_tripdata_{YYYY-MM}.parquet
- Honors environment variables (used by Bruin when running assets):
  - BRUIN_START_DATE (YYYY-MM-DD)
  - BRUIN_END_DATE (YYYY-MM-DD)
  - TAXI_TYPES: comma-separated list (e.g. "yellow,green"). If absent, defaults to ["yellow"].

The script saves files under `data/raw/<taxi_type>/` with filenames
matching the remote file names. It will skip files that already exist.

This asset keeps data raw (append-style ingestion). It only downloads files
and does not perform transformations.
"""

# import json
# import logging
# import os
# from datetime import date, datetime
# from pathlib import Path
# from typing import Iterable, List

# import requests

# logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
# logger = logging.getLogger(__name__)


# def parse_date(s: str) -> date:
#     return datetime.strptime(s, "%Y-%m-%d").date()


# def iter_months(start: date, end: date) -> Iterable[date]:
#     """Yield the first-of-month dates from start (inclusive) to end (exclusive)."""
#     cur = date(start.year, start.month, 1)
#     end_month = date(end.year, end.month, 1)
#     while cur <= end_month:
#         yield cur
#         if cur.month == 12:
#             cur = date(cur.year + 1, 1, 1)
#         else:
#             cur = date(cur.year, cur.month + 1, 1)


# def month_str(dt: date) -> str:
#     return f"{dt.year:04d}-{dt.month:02d}"


# def download_file(url: str, dest: Path, chunk_size: int = 1024 * 32) -> None:
#     dest.parent.mkdir(parents=True, exist_ok=True)
#     if dest.exists():
#         logger.info("Skipping existing file: %s", dest)
#         return
#     logger.info("Downloading %s -> %s", url, dest)
#     resp = requests.get(url, stream=True, timeout=60)
#     if resp.status_code != 200:
#         logger.warning("Failed to download %s: HTTP %s", url, resp.status_code)
#         return
#     with open(dest, "wb") as f:
#         for chunk in resp.iter_content(chunk_size=chunk_size):
#             if chunk:
#                 f.write(chunk)
#     logger.info("Saved %s", dest)


# def taxi_types_from_env() -> List[str]:
#     # Try several common places for variables set by orchestration
#     # 1) TAXI_TYPES env as CSV
#     v = os.environ.get("TAXI_TYPES")
#     if v:
#         return [t.strip() for t in v.split(",") if t.strip()]

#     # 2) BRUIN_VARS may contain JSON with taxi_types
#     bruin_vars = os.environ.get("BRUIN_VARS")
#     if bruin_vars:
#         try:
#             parsed = json.loads(bruin_vars)
#             tv = parsed.get("taxi_types")
#             if isinstance(tv, list):
#                 return [str(x) for x in tv]
#         except Exception:
#             pass

#     # 3) default
#     return ["yellow"]


# def main() -> None:
#     start = os.environ.get("BRUIN_START_DATE") or os.environ.get("START_DATE")
#     end = os.environ.get("BRUIN_END_DATE") or os.environ.get("END_DATE")
#     if not start or not end:
#         logger.info("BRUIN_START_DATE/BRUIN_END_DATE not provided, defaulting to last 1 month")
#         today = date.today()
#         start = date(today.year, max(1, today.month - 1), 1).strftime("%Y-%m-%d")
#         end = today.strftime("%Y-%m-%d")

#     start_d = parse_date(start)
#     end_d = parse_date(end)
#     if start_d > end_d:
#         raise SystemExit("BRUIN_START_DATE must be <= BRUIN_END_DATE")

#     taxi_types = taxi_types_from_env()
#     logger.info("Taxi types: %s", taxi_types)

#     base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data"
#     out_base = Path("data/raw")

#     for t in taxi_types:
#         for m in iter_months(start_d, end_d):
#             fname = f"{t}_tripdata_{month_str(m)}.parquet"
#             url = f"{base_url}/{fname}"
#             dest = out_base / t / fname
#             try:
#                 download_file(url, dest)
#             except Exception as exc:
#                 logger.exception("Error downloading %s: %s", url, exc)


# if __name__ == "__main__":
#     main()

import os
import json
import pandas as pd
from datetime import datetime

def materialize():
    start_date = os.environ["BRUIN_START_DATE"]
    end_date = os.environ["BRUIN_END_DATE"]
    taxi_types = json.loads(os.environ["BRUIN_VARS"]).get("taxi_types", ["yellow"])

    # Generate list of months between start and end dates
    # Fetch parquet files from:
    # https://d37ci6vzurychx.cloudfront.net/trip-data/{taxi_type}_tripdata_{year}-{month}.parquet
    
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")

    # Generate list of (year, month) tuples
    months = []
    current = start.replace(day=1)

    while current <= end:
        months.append((current.year, current.month))
        
        if current.month == 12:
            current = current.replace(year=current.year + 1, month=1)
        else:
            current = current.replace(month=current.month + 1)

    dataframes = []

    for taxi_type in taxi_types:
        for year, month in months:
            month_str = f"{month:02d}"
            url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{taxi_type}_tripdata_{year}-{month_str}.parquet"
            
            try:
                df = pd.read_parquet(url)
                # df["taxi_type"] = taxi_type  # Optional: helpful for downstream analysis
                dataframes.append(df)
            except Exception as e:
                print(f"Failed to load {url}: {e}")

    if not dataframes:
        return pd.DataFrame()

    final_dataframe = pd.concat(dataframes, ignore_index=True)

    return final_dataframe
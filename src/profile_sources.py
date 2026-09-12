"""Week 2 starter: profile CSV, JSON, Parquet, API payload, and PostgreSQL table.
Complete the TODOs. Do not hard-code expected counts.
"""
from pathlib import Path
import json, csv
DATA_DIR=Path(__file__).resolve().parents[1]/'data'

def profile_csv(path):
    # TODO: row count, columns, missing counts, duplicate rows, duplicate customer_id, inferred types
    pass

def profile_json(path):
    # TODO: record count, keys, nested fields, date/time fields, numeric fields, nulls
    pass

def profile_parquet(path):
    # TODO: use pandas.read_parquet; report rows/columns/dtypes/nulls and file size
    # Requires pyarrow from requirements.txt
    pass

if __name__=='__main__':
    profile_csv(DATA_DIR/'customers.csv')
    profile_json(DATA_DIR/'orders.json')
    profile_parquet(DATA_DIR/'products.parquet')

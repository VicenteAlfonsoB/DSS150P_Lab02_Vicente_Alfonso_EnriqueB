"""Week 2 starter: profile CSV, JSON, Parquet, API payload, and PostgreSQL table.
Complete the TODOs. Do not hard-code expected counts.
"""
from pathlib import Path
from collections import Counter
import json, csv
import pandas as pd

DATA_DIR = Path(__file__).resolve().parents[1] / 'data'


def _header(title):
    print("\n" + "=" * 78)
    print(title)
    print("=" * 78)


def _infer_logical_type(s):
    """Guess a logical type from the values, not just the pandas dtype."""
    non_null = s.dropna()
    if non_null.empty:
        return "unknown (all missing)"
    if pd.api.types.is_bool_dtype(s):
        return "boolean"
    if pd.api.types.is_integer_dtype(s):
        return "integer"
    if pd.api.types.is_float_dtype(s):
        return "decimal"
    if pd.api.types.is_datetime64_any_dtype(s):
        return "timestamp"
    sample = non_null.astype(str)
    try:
        parsed = pd.to_datetime(sample, errors="coerce", format="mixed")
        if parsed.notna().mean() > 0.9:
            return "date/timestamp (stored as text)"
    except Exception:
        pass
    if sample.str.contains("@", regex=False).mean() > 0.9:
        return "email (string)"
    return "string"


def profile_csv(path):
    # row count, columns, missing counts, duplicate rows, duplicate customer_id, inferred types
    _header(f"CSV PROFILE: {path.name}")

    size_bytes = path.stat().st_size
    df = pd.read_csv(path)
    rows, cols = df.shape

    print(f"File size : {size_bytes:,} bytes ({size_bytes/1024:.1f} KB)")
    print(f"Rows      : {rows:,}")
    print(f"Columns   : {cols}")

    print("\nColumn detail")
    print("-" * 78)
    print(f"{'column':<18}{'dtype':<12}{'logical type':<32}{'missing':>8}{'miss%':>7}{'uniq':>7}")
    print("-" * 78)
    for col in df.columns:
        s = df[col]
        missing = int(s.isna().sum())
        pct = missing / rows * 100 if rows else 0
        print(f"{col:<18}{str(s.dtype):<12}{_infer_logical_type(s):<32}"
              f"{missing:>8}{pct:>6.1f}%{s.nunique(dropna=True):>7}")

    placeholders = {"", " ", "na", "n/a", "null", "none", "-", "unknown", "nan"}
    print("\nPlaceholder-style values NOT counted as missing by pandas")
    print("-" * 78)
    found = False
    for col in df.columns:
        vals = df[col].dropna()
        if vals.empty:
            continue
        hits = int(vals.astype(str).str.strip().str.lower().isin(placeholders).sum())
        if hits:
            found = True
            print(f"  {col}: {hits}")
    if not found:
        print("  none found")

    exact_dupes = int(df.duplicated().sum())
    print(f"\nExact duplicate rows (every column identical): {exact_dupes}")
    if exact_dupes:
        print("  Rows involved (showing up to 10):")
        print(df[df.duplicated(keep=False)]
              .sort_values(list(df.columns))
              .head(10)
              .to_string(index=False))

    key = "customer_id"
    if key in df.columns:
        print(f"\nCandidate key check: {key}")
        print(f"  distinct values : {df[key].nunique(dropna=True):,}")
        print(f"  missing values  : {int(df[key].isna().sum())}")
        print(f"  unique overall  : {df[key].is_unique}")
        counts = df[key].value_counts()
        repeated = counts[counts > 1]
        if not repeated.empty:
            print(f"  repeated ids    : {len(repeated)}")
            conflicting = [cid for cid, grp in df[df[key].isin(repeated.index)].groupby(key)
                           if len(grp.drop_duplicates()) > 1]
            print(f"  ids repeating with DIFFERENT values: {len(conflicting)} {conflicting}")
            for cid in conflicting:
                print(df[df[key] == cid].to_string(index=False))

    print("\nFirst 5 rows")
    print("-" * 78)
    print(df.head().to_string(index=False))


def profile_json(path):
    # record count, keys, nested fields, date/time fields, numeric fields, nulls
    _header(f"JSON PROFILE: {path.name}")

    size_bytes = path.stat().st_size
    raw = json.loads(path.read_text())

    print(f"File size : {size_bytes:,} bytes ({size_bytes/1024:.1f} KB)")
    print(f"Root type : {type(raw).__name__}")
    print(f"Root is a list of records: {isinstance(raw, list)}")
    if not isinstance(raw, list):
        print("Root is not a list - stopping.")
        return

    records = raw
    n = len(records)
    print(f"Records   : {n:,}")

    key_counts = Counter()
    for r in records:
        key_counts.update(r.keys())

    print("\nTop-level keys")
    print("-" * 78)
    print(f"{'key':<18}{'present':>9}{'missing':>9}{'null':>7}   python types")
    print("-" * 78)
    for k in key_counts:
        present = key_counts[k]
        nulls = sum(1 for r in records if r.get(k) is None)
        types = sorted({type(r[k]).__name__ for r in records if k in r and r[k] is not None})
        print(f"{k:<18}{present:>9}{n - present:>9}{nulls:>7}   {','.join(types)}")

    nested = [k for k in key_counts
              if any(isinstance(r.get(k), (dict, list)) for r in records)]
    print(f"\nNested fields: {nested if nested else 'none'}")
    for k in nested:
        subkeys = Counter()
        for r in records:
            if isinstance(r.get(k), dict):
                subkeys.update(r[k].keys())
        if subkeys:
            print(f"  {k} -> sub-keys: {dict(subkeys)}")

    numeric, timestamps = [], []
    for k in key_counts:
        vals = [r[k] for r in records if k in r and r[k] is not None]
        if not vals:
            continue
        if all(isinstance(v, (int, float)) and not isinstance(v, bool) for v in vals):
            numeric.append(k)
        elif all(isinstance(v, str) for v in vals):
            try:
                parsed = pd.to_datetime(pd.Series(vals), errors="coerce", format="mixed")
                if parsed.notna().mean() > 0.9:
                    timestamps.append(k)
            except Exception:
                pass
    print(f"\nNumeric fields   : {numeric}")
    print(f"Timestamp fields : {timestamps}")

    flat = pd.json_normalize(records)
    print(f"\nFlattened shape  : {flat.shape}")
    print(f"Flattened columns: {list(flat.columns)}")
    print("\nNulls after flattening")
    print(flat.isna().sum().to_string())

    print("\nSample record")
    print("-" * 78)
    print(json.dumps(records[0], indent=2))


def profile_parquet(path):
    # use pandas.read_parquet; report rows/columns/dtypes/nulls and file size
    _header(f"PARQUET PROFILE: {path.name}")

    size_bytes = path.stat().st_size
    df = pd.read_parquet(path)
    rows, cols = df.shape

    print(f"File size : {size_bytes:,} bytes ({size_bytes/1024:.1f} KB)")
    print(f"Rows      : {rows:,}")
    print(f"Columns   : {cols}")

    print("\nColumn detail")
    print("-" * 78)
    print(f"{'column':<24}{'dtype':<18}{'missing':>9}{'uniq':>8}")
    print("-" * 78)
    for col in df.columns:
        s = df[col]
        print(f"{col:<24}{str(s.dtype):<18}{int(s.isna().sum()):>9}{s.nunique(dropna=True):>8}")

    num = df.select_dtypes(include="number")
    if not num.empty:
        print("\nNumeric summary")
        print("-" * 78)
        print(num.describe().to_string())

    try:
        import pyarrow.parquet as pq
        pf = pq.ParquetFile(path)
        md = pf.metadata
        print("\nParquet physical metadata")
        print("-" * 78)
        print(f"  row groups  : {md.num_row_groups}")
        print(f"  created by  : {md.created_by}")
        print(f"  compression : {md.row_group(0).column(0).compression}")
        print("\nArrow schema (types stored INSIDE the file):")
        print(pf.schema_arrow)
    except Exception as e:
        print(f"  (pyarrow metadata unavailable: {e})")

    print("\nSame data, three formats - file size")
    print("-" * 78)
    for name in ["products.parquet",
                 "products_optional_compare.csv",
                 "products_optional_compare.json"]:
        p = DATA_DIR / name
        if p.exists():
            b = p.stat().st_size
            print(f"  {name:<36}{b:>10,} bytes ({b/1024:>7.1f} KB)")

    csv_twin = DATA_DIR / "products_optional_compare.csv"
    if csv_twin.exists():
        csv_df = pd.read_csv(csv_twin)
        print("\nType preservation: parquet vs the same data as CSV")
        print("-" * 78)
        print(f"{'column':<24}{'parquet':<18}{'csv':<18}")
        for col in df.columns:
            c = str(csv_df[col].dtype) if col in csv_df.columns else "(absent)"
            print(f"{col:<24}{str(df[col].dtype):<18}{c:<18}")

    print("\nFirst 5 rows")
    print("-" * 78)
    print(df.head().to_string(index=False))


if __name__=='__main__':
    profile_csv(DATA_DIR/'customers.csv')
    profile_json(DATA_DIR/'orders.json')
    profile_parquet(DATA_DIR/'products.parquet')
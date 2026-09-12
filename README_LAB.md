# DSS150P Laboratory Activity #2 — Source Profiling and Rerunnable Ingestion

Vicente Alfonso Enrique B. — September 2026

## What this repository does

Profiles five data sources (CSV, JSON, Parquet, a paginated REST API, and a
PostgreSQL table), then ingests the file and API sources into a raw area in a way
that is safe to run repeatedly and safe to interrupt.

## Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Start PostgreSQL and load the ticket table:

```bash
docker compose up -d
docker exec -i dss150p-lab2-postgres psql -U dss150p -d dss150p < sql/seed_support_tickets.sql
```

## Running

The API server must be running in its own terminal before any API work:

```bash
python src/local_api_server.py
```

Then, in a second terminal:

```bash
python src/profile_sources.py | tee outputs/profiling_output.txt   # Part 1
python src/ingest_pipeline.py                                      # Part 3
python src/validate_raw.py                                         # validation
```

Run `ingest_pipeline.py` twice to observe idempotency: the second run skips all
three files on content hash, fetches 0 events because the watermark filters them,
and leaves the raw record count and watermark unchanged.

To verify reproducibility from scratch:

```bash
rm -rf raw state
python src/ingest_pipeline.py
python src/validate_raw.py
```

`raw/` and `state/` are generated and git-ignored. `outputs/` is committed because
it holds the evidence.

## Results

| Source | Records | Notes |
|---|---:|---|
| `customers.csv` | 250 | `customer_id` not unique (247 distinct); 2 exact duplicate rows; C0090 is an identity collision |
| `orders.json` | 250 | `order_id` unique; `total_amount` reconciles exactly on all rows |
| `products.parquet` | 200 | `product_id` unique; schema embedded in file; uncompressed |
| API `/api/events` | 122 → 120 | 5 pages at 25/page; E0020 and E0055 restated, deduplicated to newest |
| `support_tickets` | 250 | `ticket_id` is an enforced primary key; inspection only |

## Deliverables

| Path | Contents |
|---|---|
| `outputs/profiling_report.md` | Source profiling report (Part 1) |
| `outputs/profiling_output.txt` | Raw profiling run output |
| `outputs/ingestion_design.md` | Ingestion design and watermark semantics (Tasks 2.4–2.5) |
| `outputs/reflection.md` | Reflection answers (Section 9) |
| `outputs/pipeline_run_log.csv` | One row per source per run |
| `outputs/screenshots/` | Evidence screenshots |
| `config/source_metadata.yml` | Metadata inventory for all five sources |
| `config/logical_schema.yml` | Logical schema for customers and API events |
| `config/data_contract_customers.yml` | Completed data contract |

## Design decisions

Files are copied whole because none carries a change marker; rerun safety comes
from SHA-256 content hashing, and raw filenames embed the hash so a changed source
lands beside its predecessor instead of overwriting it.

The API is incremental because it exposes both `updated_at` and `updated_after`.
Deduplication keeps the greatest `updated_at` per `event_id`, derived at runtime
with no event ID hard-coded anywhere.

The watermark is written only after `os.replace` has durably swapped the raw file
into place. A failure anywhere earlier leaves it untouched, so the next run
refetches rather than skipping — the worst case is reprocessing, never data loss.
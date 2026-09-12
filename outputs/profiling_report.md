# Source Profiling Report

**Course:** DSS150P — Integrated Laboratory Activity #2
**Author:** Vicente Alfonso Enrique B.
**Date:** 2026-09-12
**Environment:** Python 3.14.2, pandas 3.0.5, pyarrow 25.0.1, PostgreSQL 16 (Docker), local REST API at 127.0.0.1:8000

All figures below were produced by `src/profile_sources.py`; the raw run output is preserved in `outputs/profiling_output.txt`. No source file was modified.

## 1. Source Inventory

| Source | Type | Rows/Records | Key | Update Pattern | Quality Findings |
|---|---|---:|---|---|---|
| `data/customers.csv` | Delimited text, 17.8 KB | 250 rows × 7 cols | `customer_id` — candidate key, **not unique** (247 distinct) | Full snapshot; no change-marker column | 2 exact duplicate rows; C0090 held by two different people; 3 missing `email`, 2 missing `city` |
| `data/orders.json` | JSON array of objects, 76.1 KB | 250 records × 9 keys | `order_id` — unique (250/250) | Full snapshot; `order_timestamp` present, no update marker | Nested `shipping` object; timezone-naive timestamps; zero nulls and zero missing keys; `total_amount` reconciles exactly to `subtotal + shipping_fee` on all 250 records |
| `data/products.parquet` | Parquet, 1 row group, uncompressed, 14.3 KB | 200 rows × 7 cols | `product_id` — unique (200/200) | Full snapshot | No nulls; schema embedded in file; larger on disk than the equivalent CSV |
| Local REST API `/api/events` | Paginated HTTP JSON | 122 events across 5 pages @ 25/page | `event_id` — **not unique** (120 distinct) | Incremental; supports `updated_after`; per-record `updated_at` | 2 restated events (E0020, E0055) with newer timestamps and changed amounts; timezone-naive `updated_at` |
| PostgreSQL `support_tickets` | Relational table, PostgreSQL 16 | 250 rows × 8 cols | `ticket_id` — enforced PRIMARY KEY (btree index) | Timestamped via `opened_at` / `resolved_at` | 4 rows with NULL `assigned_agent`; 125 rows with NULL `resolved_at` (not-applicable rather than missing); `timestamp without time zone` |

## 2. Schema Findings

The five sources declare their structure with very different degrees of rigour, and that difference is the single most useful thing profiling revealed.

The Parquet file carries its own schema inside the file. `pyarrow` reports `product_id: string not null`, `stock_quantity: int32 not null`, `unit_price: double not null`, and so on — types and nullability are properties of the data, not of the reader. Reading the identical data from `products_optional_compare.csv` widens `stock_quantity` from `int32` to `int64`, because pandas infers the type at read time from the values present. The same file read next month, after the values change, could infer differently. CSV therefore provides no schema guarantee at all; it provides a delimiter.

PostgreSQL sits at the other end. `support_tickets` declares column types, lengths, `NOT NULL` constraints, and a primary key backed by a btree index. Uniqueness of `ticket_id` is not something the pipeline must verify — the database has already refused to store a violation. Contrast this with `customer_id` in `customers.csv`, which plays exactly the same conceptual role but is repeated three times across 250 rows. The key did not fail; nothing was ever enforcing it.

`orders.json` is self-describing per record but structurally unconstrained: keys may vary between records, and types are whatever JSON encodes. In practice all 250 records carry all 9 keys with consistent types, but nothing in the format guarantees that for record 251. The `shipping` field is the only nested structure, a two-key object (`region`, `method`). Flattening with `json_normalize` produces a 10-column table with `shipping.region` and `shipping.method`.

The API returns the same shape as the JSON file — flat scalars plus a nested `metadata` object (`channel`, `campaign`) — wrapped in a pagination envelope of `page`, `per_page`, `total`, `has_more`, `next_page`, and `items`.

One defect is shared by three of the five sources. `orders.json.order_timestamp`, the API's `updated_at`, and Postgres `opened_at` / `resolved_at` are all timezone-naive: `2026-08-01T11:00:00`, and the Postgres column is literally typed `timestamp without time zone`. No source states what clock produced these values. Any comparison across sources, and any watermark arithmetic, rests on an assumption nobody wrote down.

## 3. Data Quality Findings

**`customer_id` does not identify a customer.** 250 rows hold 247 distinct IDs. Two of the repeats are exact duplicate rows — C0036 (Kevin Fernandez) and C0145 (Julia Tan) appear twice with every field identical. The third is different in kind: C0090 appears once as Paolo Aquino, Marikina, SME, signed up 2025-07-30, and once as Hannah Reyes, Caloocan, Retail, signed up 2026-04-22. These are two different people sharing an identifier. An exact duplicate can be dropped without losing information; this cannot, and no automated rule can resolve it without a decision from the source owner.

**`email` cannot serve as a fallback key.** It holds 184 distinct values across 247 non-missing rows, and is absent in 3 rows entirely. The file draws from only 20 first names and 20 last names, so addresses collide by construction.

**The API restates events rather than duplicating them.** `event_id` E0020 appears with amount 651.71 at `2026-08-03T20:00:00` and again with 999.99 at `2026-08-21T09:00:00`. E0055 appears with 1401.35 at `2026-08-08T05:00:00` and 888.88 at `2026-08-21T10:00:00`. In both cases the later record carries a different amount. These are corrections, not transmission artefacts, so deduplication must be a deterministic keep-newest rule ordered by `updated_at` — dropping an arbitrary copy would silently select the stale value half the time.

**The newest record in the feed is itself a duplicate.** The maximum `updated_at` across all 122 events is `2026-08-21T10:00:00`, which belongs to E0055's restated version. Deduplication and watermarking are therefore coupled: a dedup rule that kept the older row would compute a watermark of `2026-08-21T09:00:00` and cause the next run to re-fetch data already held.

**Nullability carries two different meanings in `support_tickets`.** `assigned_agent` is NULL on 4 rows — a genuine gap, a ticket nobody owns. `resolved_at` is NULL on 125 rows, which is not a gap at all: it aligns exactly with status. Closed (61) and Resolved (64) rows all carry a resolution timestamp; Open (56) and In Progress (69) rows carry none. 61 + 64 = 125 present, 56 + 69 = 125 absent. Treating both columns as "missing data" and imputing them would corrupt the meaning of the second.

**No timestamp inversions exist.** `COUNT(*) FILTER (WHERE resolved_at < opened_at)` returns 0 across all 250 tickets, and `opened_at` spans 2026-01-01 15:00 to 2026-06-29 16:00. This is reported as a check that passed; profiling that only records faults gives a misleading picture of a source.

**`orders.json` is internally consistent and correctly keyed.** `order_id` is unique across all 250 records, making it a true candidate key — notable because `customer_id` failed the identical test in `customers.csv`. `total_amount` equals `subtotal + shipping_fee` on every record with zero discrepancies, which means the total is derived rather than independently captured. Both facts are reported as checks that passed, and both are strong enough to state as contract rules rather than expectations. `status` draws from a closed set of six values (Cancelled, Delivered, Packed, Paid, Pending, Shipped), and `order_timestamp` spans 2026-01-02 03:00 to 2026-06-30 07:38.

**Storage format changes type fidelity and size in opposite directions.** `products.parquet` (14.3 KB) is larger than the same data as CSV (11.3 KB) and far smaller than as JSON (39.7 KB). The Parquet file is written uncompressed in a single row group at only 200 rows, so schema and footer overhead outweighs any encoding benefit at this scale. Its advantage here is not size but type fidelity: `int32` survives a round trip, where CSV does not preserve it.

## 4. Recommended Acquisition Method

| Source | Method | Raw destination | Duplicate key | Incremental state |
|---|---|---|---|---|
| `customers.csv`, `orders.json`, `products.parquet` | Byte-for-byte file copy plus a manifest recording name, size, SHA-256, and ingestion timestamp | `raw/files/` | File content SHA-256 | Not applicable — full snapshot each run |
| Local REST API `/api/events` | Paginated GET, looping while `has_more` is true | `raw/api/events.jsonl` | `event_id`, keeping the greatest `updated_at` | `max(updated_at)` persisted to `state/api_watermark.json` |
| PostgreSQL `support_tickets` | Inspection only in this laboratory | Not applicable | `ticket_id` | Would require a timestamp or CDC strategy; see below |

The three files carry no change indicator of any kind, so there is nothing to read incrementally against — a full copy per run is the only correct option, and content hashing is what prevents a rerun from writing a second identical copy. The API is the opposite case: it exposes both a per-record `updated_at` and an `updated_after` request parameter, so incremental retrieval is both possible and cheaper.

Extending to `support_tickets` would be harder than it looks. `opened_at` is immutable and therefore useless as a change marker, because a ticket's `status`, `assigned_agent`, and `resolved_at` all mutate long after it was opened. A watermark over `opened_at` would capture new tickets and permanently miss every update to existing ones. Doing this properly needs either a maintained `updated_at` column with a trigger, or log-based change data capture.

## 5. Risks and Assumptions

**The watermark strategy is currently safe by luck, not by design.** Exactly one record holds the maximum `updated_at`, so requesting `updated_after > watermark` on the next run loses nothing. If two records ever shared that timestamp and only one had been written before a failure, the strict inequality would skip the other permanently. A production system would use a composite cursor of `(updated_at, event_id)` or re-request a small overlap window and rely on deduplication.

**All timestamp comparisons assume a single implicit timezone.** No source declares one. If any of these systems ever runs in a different zone, or observes daylight saving, the watermark and any cross-source join become silently wrong. The correct fix is at the source; the mitigation available to this pipeline is to record `_ingested_at` explicitly in UTC so at least the acquisition time is unambiguous.

**Profiled volumes are small and will not reveal scale problems.** 250 rows, 200 rows, and 122 events are trivial; loading each source fully into memory is fine here and would not be at production scale. The API's `total` of 122 is likewise small enough that pagination bugs may not surface — code that mishandles the final partial page, for example, is easy to miss when there are only five pages.

**The `customer_id` collision on C0090 has no automated resolution.** Both rows are plausible and neither carries a timestamp indicating which is current. The raw area must preserve both, and the decision belongs to whoever owns the source system.

**The closed value sets observed may not be closed in future deliveries.** `customer_segment` has 4 values, `status` in `orders.json` has 6, `category` and `brand` in the product data have 6 each. These are facts about this snapshot, not guarantees. Pinning them as enum constraints in a contract is correct, but the contract must also say what happens when a new value arrives — reject, quarantine, or accept and alert.
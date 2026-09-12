# Ingestion Design and Watermark Semantics

**Course:** DSS150P — Integrated Laboratory Activity #2 2.4 and 2.5
**Name** Vicente Alfonso Enrique B.


Written before any pipeline code, from the evidence in `outputs/profiling_output.txt`.

## 1. Ingestion Design

| Source | Method | Raw destination | Duplicate key | Incremental state |
|---|---|---|---|---|
| `customers.csv`, `orders.json`, `products.parquet` | File copy plus manifest entry | `raw/files/` | File content SHA-256 | Not applicable |
| Local REST API `/api/events` | Paginated GET, looping while `has_more` is true | `raw/api/events.jsonl` | `event_id`, keeping greatest `updated_at` | `max(updated_at)` in `state/api_watermark.json` |
| PostgreSQL `support_tickets` | Inspection only in this laboratory | Not applicable | `ticket_id` | Would require a maintained `updated_at` column or log-based CDC |

### Why the files are full copies

None of the three files carries a change marker. There is no `updated_at`, no version number, no sequence, and no delete indicator. `signup_date` and `order_timestamp` look superficially like candidates but both record when a business event happened, not when the row was last modified — a customer who changes city keeps their original `signup_date`, so a watermark built on it would never see the edit. `products.parquet` has no timestamp at all.

With nothing to compare against, incremental reading is not merely inconvenient, it is undefined. A full copy per run is the only correct option.

Rerun safety therefore cannot come from record keys, because a full snapshot legitimately contains every record every time. It comes from content instead: the SHA-256 of each file is computed, checked against the manifest, and the copy is skipped when the hash is already recorded. Identical input produces no new output.

### Why the API is incremental

The API is the opposite case. It exposes a per-record `updated_at` and accepts an `updated_after` query parameter, so the source itself can filter. Requesting only what changed is cheaper for both sides and is what the endpoint was designed for.

Deduplication here is by `event_id`, keeping the record with the greatest `updated_at`. This is not optional tidying. The feed delivers E0020 twice with amounts 651.71 and 999.99, and E0055 twice with 1401.35 and 888.88. The later record in each pair is a restatement carrying a corrected value, so an arbitrary keep-one rule would select the stale figure roughly half the time and do so silently.

### Why `support_tickets` is inspection only

The table is mutable in ways its timestamps do not record. `opened_at` is set once and never changes, while `status`, `assigned_agent` and `resolved_at` all change afterwards. A watermark over `opened_at` would capture new tickets and permanently miss every update to existing ones — the failure would be invisible, because new rows would keep arriving and the pipeline would look healthy.

Doing this properly needs either a maintained `updated_at` column backed by a trigger, or log-based change data capture reading the write-ahead log. Both are out of scope here, which is precisely why the lab restricts this source to inspection.

## 2. Watermark Semantics

For this laboratory the API watermark is **the greatest `updated_at` value that has been successfully persisted to the raw area**. On the next run the pipeline requests only records whose `updated_at` is greater than the saved watermark.

The phrase *successfully persisted* is doing all the work. The watermark is a claim about what is durably stored — not about what was fetched, parsed, deduplicated, or held in memory. Every one of those intermediate states can be lost; only the write survives a crash. It is operational state belonging to the pipeline rather than source data, which is why it lives in `state/` and is excluded from version control. Deleting it is not data loss. It causes a full re-fetch, and deduplication makes that a non-event.

### What could go wrong if the watermark is saved before the raw file is successfully written?

Records are skipped permanently, and nothing reports it. I would call this a bug rather than a risk, and I would call it a bug even in a pipeline that has never actually failed — the ordering of those two writes is not a best practice to adopt when convenient, it is what makes the watermark mean anything at all. A watermark written before the data it describes is simply a lie that has not been caught yet.

The mechanism is straightforward. The watermark advances to cover records that were never stored. The next run asks for `updated_after > watermark`, the source correctly returns nothing in that range, and the pipeline reports success. Those records are never requested again — not next run, not ever — because from the pipeline's own point of view they were handled.

Made concrete with this feed: the current maximum `updated_at` is `2026-08-21T10:00:00`, belonging to E0055's restated version carrying amount 888.88. Save that watermark first, fail on the write, and the next run requests events after `10:00:00` and gets an empty set. The correction never lands. The raw area holds the stale 1401.35 forever, and every run afterwards is green.

That is strictly worse than crashing. A crash is loud, attributable, and retried. This produces a pipeline that is quietly and permanently wrong while reporting success — the single worst outcome available to a data system, because the failure is discovered months later by someone who trusted the numbers.

The correct ordering is: fetch every page, deduplicate, write the raw output durably, then advance the watermark. What that ordering buys is a guarantee about the *shape* of failure — the worst case becomes reprocessing rather than skipping. Reprocessing is absorbed by deduplication and costs nothing but time. Skipping is unrecoverable without manual intervention nobody knows is needed. Given a choice between a system that sometimes does too much and one that sometimes does too little, the first is the only defensible option.

### What could go wrong if the source allows multiple records with exactly the same timestamp?

Strict `>` stops being safe, and my position is that `>` on a bare timestamp cursor is wrong in principle rather than merely risky in practice.

The failure: suppose five records share `updated_at = 2026-08-21T10:00:00` and the process dies after two are written. The watermark, correctly computed from what was persisted, is `10:00:00`. The next run requests `updated_after > 10:00:00`, which excludes all five — including the three that were never stored. They are lost silently. Note that the operation ordering here was *correct*; the bug has simply moved from the sequence of writes into the comparison operator.

Ties are not an edge case. Batch jobs stamp entire batches with one timestamp, and any source with second-level granularity produces collisions as soon as volume rises. Designing around the assumption that timestamps are unique is designing around an assumption the source never made.

In the current feed exactly one record holds the maximum `updated_at`, so this never fires. I would not treat that as reassurance. The code is correct today by accident, and accidental correctness has no failure mode you can anticipate — it simply stops being true one day, without warning and without a code change to blame.

Switching to `>=` trades silent data loss for guaranteed reprocessing of at least one record per run. That is the right trade, and it is only acceptable because deduplication is idempotent. But it should be named for what it is: a mitigation that makes the wrong cursor survivable, not a fix for it.

### One limitation of this simplified watermark, and one production-grade mitigation

**Limitation.** A scalar timestamp is the wrong data type for this job. It collapses ordering and identity into a single value, so any set of records sharing a timestamp is atomic to the cursor — all of them are either before it or after it, and a partial write within that set cannot be represented at all. There is no way to express "I have three of the five records at 10:00:00", which means there is no way to resume correctly from one.

**Mitigation.** Use a **composite cursor of `(updated_at, event_id)`** compared lexicographically, with the source ordering and filtering on that pair. Because `event_id` is unique within any timestamp, the pair gives a total order with no ties, and the cursor can land precisely between two records sharing an `updated_at`. Resumption after a partial write continues from exactly the right record — no skipping, no blanket reprocessing. This is the correct fix, and it costs almost nothing: one extra field in the state file and a compound comparison.

Two further measures belong with it. Write the raw output atomically — temporary file, then rename over the target, since rename is atomic on POSIX filesystems — so a truncated file can never be mistaken for a complete one. And where the store supports it, commit the raw data and the state update in a single transaction, which eliminates the window between them rather than merely ordering it. Ordering the writes is the cheap version of the guarantee; a transaction is the real one.

At the strongest end, log-based change data capture abandons timestamps entirely in favour of a monotonically increasing log sequence number — unique, totally ordered, and generated by the database rather than by application code that has to be trusted to get it right. If I were building this for production rather than for a laboratory, that is where I would start, and the timestamp watermark would never be written at all.
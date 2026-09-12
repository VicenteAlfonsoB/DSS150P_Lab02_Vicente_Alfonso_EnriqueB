"""Week 3 starter: rerunnable ingestion to a raw area.
Students implement file ingestion + paginated REST API ingestion + watermark + duplicate prevention.
"""
from pathlib import Path
from datetime import datetime, timezone
import json, hashlib, shutil
import os, csv, uuid
import requests

ROOT=Path(__file__).resolve().parents[1]
DATA=ROOT/'data'; RAW=ROOT/'raw'; STATE=ROOT/'state'
API_URL='http://127.0.0.1:8000/api/events'

OUTPUTS = ROOT/'outputs'
RUN_LOG = OUTPUTS/'pipeline_run_log.csv'
LOG_TEMPLATE = ROOT/'templates'/'pipeline_run_log_template.csv'

def utc_now(): return datetime.now(timezone.utc).isoformat()

def sha256_file(path):
    h=hashlib.sha256()
    with path.open('rb') as f:
        for chunk in iter(lambda:f.read(1024*1024),b''): h.update(chunk)
    return h.hexdigest()

def load_watermark():
    p=STATE/'api_watermark.json'
    if not p.exists(): return None
    return json.loads(p.read_text())['updated_at']

def save_watermark(value):
    """Persist the watermark atomically: write a temp file, then rename over the target.

    rename is atomic on POSIX filesystems, so a crash mid-write can never leave a
    half-written state file that a later run would parse as authoritative.
    """
    STATE.mkdir(exist_ok=True)
    target = STATE/'api_watermark.json'
    tmp = target.with_suffix('.json.tmp')
    tmp.write_text(json.dumps({'updated_at': value}, indent=2))
    os.replace(tmp, target)


# ---------------------------------------------------------------- run logging

def _log_header():
    """Read the column spec from the supplied template rather than hard-coding it."""
    return LOG_TEMPLATE.read_text().strip().splitlines()[0].split(',')

def append_run_log(row):
    header = _log_header()
    OUTPUTS.mkdir(parents=True, exist_ok=True)
    is_new = not RUN_LOG.exists()
    with RUN_LOG.open('a', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=header)
        if is_new:
            writer.writeheader()
        writer.writerow({col: row.get(col, '') for col in header})


# ------------------------------------------------------------ file ingestion

def ingest_files(run_id):
    """Copy source files into raw/files/ without creating duplicates on rerun.

    Rerun safety comes from CONTENT, not filenames. Each file's SHA-256 is checked
    against the manifest; an already-recorded hash is skipped. The raw filename
    embeds the hash, so a changed source lands beside its predecessor instead of
    silently overwriting it.
    """
    started = utc_now()
    out_dir = RAW/'files'
    manifest = out_dir/'manifest.jsonl'
    source_files = ['customers.csv', 'orders.json', 'products.parquet']
    read = written = skipped = 0

    try:
        out_dir.mkdir(parents=True, exist_ok=True)

        known_hashes = set()
        if manifest.exists():
            for line in manifest.read_text().splitlines():
                if line.strip():
                    known_hashes.add(json.loads(line)['sha256'])

        new_entries = []
        for name in source_files:
            src = DATA/name
            if not src.exists():
                raise FileNotFoundError(f'expected source file is missing: {src}')
            read += 1

            digest = sha256_file(src)
            if digest in known_hashes:
                skipped += 1
                print(f'  skip  {name:<22} content hash already ingested ({digest[:12]})')
                continue

            dest = out_dir/f'{src.stem}__{digest[:12]}{src.suffix}'
            tmp = dest.with_name(dest.name + '.tmp')
            shutil.copy2(src, tmp)
            os.replace(tmp, dest)

            new_entries.append({
                'source_file': name,
                'raw_file': dest.name,
                'ingested_at': utc_now(),
                'bytes': src.stat().st_size,
                'sha256': digest,
            })
            known_hashes.add(digest)
            written += 1
            print(f'  copy  {name:<22} -> {dest.name}')

        if new_entries:
            with manifest.open('a') as f:
                for entry in new_entries:
                    f.write(json.dumps(entry) + '\n')

        print(f'  files: read={read} written={written} skipped_as_duplicate={skipped}')
        append_run_log({
            'run_id': run_id, 'started_at': started, 'finished_at': utc_now(),
            'status': 'SUCCESS', 'source': 'files',
            'records_read': read, 'records_written': written,
            'duplicates_removed': skipped,
            'watermark_before': '', 'watermark_after': '', 'error_message': '',
        })

    except Exception as exc:
        append_run_log({
            'run_id': run_id, 'started_at': started, 'finished_at': utc_now(),
            'status': 'FAILED', 'source': 'files',
            'records_read': read, 'records_written': written,
            'duplicates_removed': skipped,
            'watermark_before': '', 'watermark_after': '',
            'error_message': f'{exc.__class__.__name__}: {exc}',
        })
        raise


def fetch_api_page(page, per_page=20, updated_after=None):
    params={'page':page,'per_page':per_page}
    if updated_after: params['updated_after']=updated_after
    r=requests.get(API_URL,params=params,timeout=30); r.raise_for_status(); return r.json()


# ------------------------------------------------------------- deduplication

def dedupe_latest(records, key='event_id', order_by='updated_at'):
    """Keep exactly one record per key: the one with the greatest order_by value.

    No event_id is named anywhere. The rule is derived from the data at runtime,
    so it works on duplicates nobody knew about.

    ISO-8601 strings compare lexicographically in the same order they compare
    chronologically, so a plain > is correct here without parsing to datetimes.

    On an exact tie the incumbent is kept, which makes the outcome deterministic
    for a given input order rather than dependent on dict iteration.
    """
    best = {}
    for rec in records:
        k = rec[key]
        current = best.get(k)
        if current is None or rec[order_by] > current[order_by]:
            best[k] = rec
    return list(best.values())


# ------------------------------------------------------------- API ingestion

def ingest_api(run_id, per_page=25):
    """Paginated retrieval, deduplication, atomic write, then watermark advance.

    Order matters and is the point of the whole exercise: the watermark is saved
    only after os.replace has durably swapped the raw file into place. Failing
    anywhere before that leaves the watermark untouched, so the next run refetches
    rather than skipping.
    """
    started = utc_now()
    out_dir = RAW/'api'
    out_file = out_dir/'events.jsonl'
    watermark_before = load_watermark()
    read = written = removed = 0
    watermark_after = watermark_before

    try:
        out_dir.mkdir(parents=True, exist_ok=True)
        print(f'  watermark before: {watermark_before or "(none - full load)"}')

        fetched, page, pages = [], 1, 0
        while True:
            try:
                payload = fetch_api_page(page, per_page=per_page, updated_after=watermark_before)
            except requests.RequestException as exc:
                raise RuntimeError(
                    f'API request failed on page {page} at {API_URL} - '
                    f'{exc.__class__.__name__}: {exc}. '
                    f'Is src/local_api_server.py running?'
                ) from exc

            items = payload.get('items', [])
            fetched.extend(items)
            pages += 1
            print(f'  page {payload.get("page"):>3}: {len(items):>3} items  '
                  f'has_more={payload.get("has_more")}')

            if not payload.get('has_more'):
                break
            page = payload.get('next_page') or page + 1
            if pages > 500:
                raise RuntimeError('pagination failed to terminate after 500 pages')

        read = len(fetched)

        ingested_at = utc_now()
        for rec in fetched:
            rec['_ingested_at'] = ingested_at
            rec['_source'] = API_URL

        existing = []
        if out_file.exists():
            for line in out_file.read_text().splitlines():
                if line.strip():
                    existing.append(json.loads(line))

        combined = existing + fetched
        deduped = dedupe_latest(combined)
        removed = len(combined) - len(deduped)
        deduped.sort(key=lambda r: (r['updated_at'], r['event_id']))

        tmp = out_file.with_name(out_file.name + '.tmp')
        with tmp.open('w') as f:
            for rec in deduped:
                f.write(json.dumps(rec) + '\n')
        os.replace(tmp, out_file)
        written = len(deduped)

        candidate = max((r['updated_at'] for r in deduped), default=None)
        if candidate is not None:
            watermark_after = candidate if watermark_before is None else max(candidate, watermark_before)
            save_watermark(watermark_after)

        print(f'  api: fetched={read} written={written} duplicates_removed={removed}')
        print(f'  watermark after : {watermark_after or "(none)"}')

        append_run_log({
            'run_id': run_id, 'started_at': started, 'finished_at': utc_now(),
            'status': 'SUCCESS', 'source': 'api',
            'records_read': read, 'records_written': written,
            'duplicates_removed': removed,
            'watermark_before': watermark_before or '',
            'watermark_after': watermark_after or '',
            'error_message': '',
        })

    except Exception as exc:
        append_run_log({
            'run_id': run_id, 'started_at': started, 'finished_at': utc_now(),
            'status': 'FAILED', 'source': 'api',
            'records_read': read, 'records_written': written,
            'duplicates_removed': removed,
            'watermark_before': watermark_before or '',
            'watermark_after': watermark_before or '',
            'error_message': f'{exc.__class__.__name__}: {exc}',
        })
        raise


if __name__=='__main__':
    RAW.mkdir(exist_ok=True); STATE.mkdir(exist_ok=True)
    run_id = uuid.uuid4().hex[:12]
    print(f'\n=== ingestion run {run_id} ===')

    failures = []
    try:
        print('\n[files]')
        ingest_files(run_id)
    except Exception as exc:
        failures.append(f'files: {exc}')
        print(f'  FAILED - {exc}')

    try:
        print('\n[api]')
        ingest_api(run_id)
    except Exception as exc:
        failures.append(f'api: {exc}')
        print(f'  FAILED - {exc}')

    print()
    if failures:
        print(f'=== run {run_id} FAILED ===')
        for f in failures:
            print(f'  {f}')
        print('  watermark was NOT advanced for any failed source.')
        raise SystemExit(1)
    print(f'=== run {run_id} SUCCESS ===')
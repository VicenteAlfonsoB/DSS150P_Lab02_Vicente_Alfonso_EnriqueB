"""Starter validation checks for raw outputs."""
from pathlib import Path
from datetime import datetime
import json, hashlib, csv, sys
ROOT=Path(__file__).resolve().parents[1]

RAW = ROOT/'raw'
STATE = ROOT/'state'
OUTPUTS = ROOT/'outputs'
DATA = ROOT/'data'

RESULTS = []


def check(name, passed, detail=''):
    RESULTS.append((name, bool(passed), detail))
    mark = 'PASS' if passed else 'FAIL'
    print(f'  [{mark}] {name}' + (f'  - {detail}' if detail else ''))
    return bool(passed)


def sha256_file(path):
    h = hashlib.sha256()
    with path.open('rb') as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b''):
            h.update(chunk)
    return h.hexdigest()


def parses_as_iso(value):
    try:
        datetime.fromisoformat(value)
        return True
    except (TypeError, ValueError):
        return False


def validate_files():
    print('\n[raw/files]')
    files_dir = RAW/'files'
    manifest = files_dir/'manifest.jsonl'

    if not check('raw/files/ exists', files_dir.is_dir()):
        return
    if not check('manifest.jsonl exists', manifest.is_file()):
        return

    entries = [json.loads(line) for line in manifest.read_text().splitlines() if line.strip()]
    check('manifest is not empty', len(entries) > 0, f'{len(entries)} entries')

    required = {'source_file', 'ingested_at', 'sha256', 'bytes'}
    missing_fields = [e for e in entries if not required.issubset(e)]
    check('every manifest entry has source_file, ingested_at, sha256, bytes',
          not missing_fields, f'{len(missing_fields)} incomplete')

    check('every ingested_at is a parseable timestamp',
          all(parses_as_iso(e.get('ingested_at', '')) for e in entries))

    check('every ingested_at is timezone-aware UTC',
          all(datetime.fromisoformat(e['ingested_at']).utcoffset() is not None
              for e in entries if parses_as_iso(e.get('ingested_at', ''))))

    missing_on_disk = [e['raw_file'] for e in entries if not (files_dir/e['raw_file']).is_file()]
    check('every manifest entry has its file on disk',
          not missing_on_disk, f'missing: {missing_on_disk}' if missing_on_disk else '')

    mismatched = []
    for e in entries:
        p = files_dir/e['raw_file']
        if p.is_file() and sha256_file(p) != e['sha256']:
            mismatched.append(e['raw_file'])
    check('every raw copy matches its recorded SHA-256',
          not mismatched, f'mismatched: {mismatched}' if mismatched else 'content verified')

    hashes = [e['sha256'] for e in entries]
    check('no content hash was ingested twice',
          len(hashes) == len(set(hashes)),
          f'{len(hashes)} entries, {len(set(hashes))} distinct hashes')

    expected_sources = {'customers.csv', 'orders.json', 'products.parquet'}
    present = {e['source_file'] for e in entries}
    check('all three source files are represented',
          expected_sources.issubset(present),
          f'missing: {sorted(expected_sources - present)}' if not expected_sources.issubset(present) else '')

    for e in entries:
        src = DATA/e['source_file']
        if src.is_file() and sha256_file(src) == e['sha256']:
            check(f"source {e['source_file']} is byte-identical to its raw copy", True)


def validate_api():
    print('\n[raw/api]')
    events_file = RAW/'api'/'events.jsonl'

    if not check('raw/api/events.jsonl exists', events_file.is_file()):
        return

    records, bad_lines = [], 0
    for line in events_file.read_text().splitlines():
        if not line.strip():
            continue
        try:
            records.append(json.loads(line))
        except json.JSONDecodeError:
            bad_lines += 1
    check('every line is valid JSON', bad_lines == 0, f'{bad_lines} malformed')
    check('raw file is not empty', len(records) > 0, f'{len(records)} records')

    ids = [r.get('event_id') for r in records]
    check('event_id is present on every record', all(ids))
    check('event_id is unique after deduplication',
          len(ids) == len(set(ids)),
          f'{len(ids)} records, {len(set(ids))} distinct ids')

    check('_ingested_at present on every record',
          all('_ingested_at' in r for r in records))
    check('_source present on every record',
          all('_source' in r for r in records))
    check('_ingested_at is timezone-aware UTC',
          all(parses_as_iso(r.get('_ingested_at', '')) and
              datetime.fromisoformat(r['_ingested_at']).utcoffset() is not None
              for r in records))

    check('updated_at parses on every record',
          all(parses_as_iso(r.get('updated_at', '')) for r in records))

    print('\n[state]')
    wm_file = STATE/'api_watermark.json'
    if not check('state/api_watermark.json exists', wm_file.is_file()):
        return

    watermark = json.loads(wm_file.read_text()).get('updated_at')
    observed_max = max((r['updated_at'] for r in records), default=None)
    check('watermark equals max(updated_at) in the raw file',
          watermark == observed_max,
          f'watermark={watermark} max={observed_max}')

    for eid in {i for i in ids if ids.count(i) > 1}:
        check(f'duplicate survived for {eid}', False)


def validate_run_log():
    print('\n[run log]')
    log = OUTPUTS/'pipeline_run_log.csv'
    template = ROOT/'templates'/'pipeline_run_log_template.csv'

    if not check('outputs/pipeline_run_log.csv exists', log.is_file()):
        return

    expected = template.read_text().strip().splitlines()[0].split(',')
    with log.open() as f:
        rows = list(csv.DictReader(f))
        header = list(rows[0].keys()) if rows else []

    check('run log header matches the supplied template',
          header == expected,
          f'expected {expected}' if header != expected else '')
    check('run log has at least one row', len(rows) > 0, f'{len(rows)} rows')
    check('every row records a status',
          all(r.get('status') in {'SUCCESS', 'FAILED'} for r in rows))

    failed = [r for r in rows if r.get('status') == 'FAILED']
    check('every failed row left the watermark unchanged',
          all(r.get('watermark_before') == r.get('watermark_after') for r in failed),
          f'{len(failed)} failed runs checked')


def validate_no_temp_files():
    print('\n[hygiene]')
    leftovers = [str(p.relative_to(ROOT)) for p in list(RAW.rglob('*.tmp')) + list(STATE.rglob('*.tmp'))]
    check('no .tmp files left behind by atomic writes',
          not leftovers, f'found: {leftovers}' if leftovers else '')


def main():
    print('=' * 70)
    print('RAW AREA VALIDATION')
    print('=' * 70)

    validate_files()
    validate_api()
    validate_run_log()
    validate_no_temp_files()

    passed = sum(1 for _, ok, _ in RESULTS if ok)
    failed = sum(1 for _, ok, _ in RESULTS if not ok)

    print('\n' + '=' * 70)
    print(f'{passed} passed, {failed} failed')
    print('=' * 70)

    if failed:
        print('\nFailed checks:')
        for name, ok, detail in RESULTS:
            if not ok:
                print(f'  - {name}' + (f' ({detail})' if detail else ''))
        sys.exit(1)
    print('All checks passed.')


if __name__=='__main__': main()
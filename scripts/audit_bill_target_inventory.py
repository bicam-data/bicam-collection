"""Bounded, read-only target inventory for the three adjudicated bill cases.

Frozen CSVs are streamed and verified; the optional local query uses explicit
read-only/time/memory limits. No raw payloads, API keys or DSNs are emitted.
"""
from __future__ import annotations

import argparse
import csv
from datetime import datetime, timezone
import gzip
import hashlib
import json
from pathlib import Path
import sys

import psycopg
from psycopg.rows import dict_row

from bicam_collection.config import Settings


BILLS = ['hr3426-119', 'hr7440-116', 'hres15-109']
FIELDS = ['bill_id', 'govinfo_package_id', 'version_code', 'version_variant', 'issued_at', 'last_modified']


def frozen_inventory(path):
    manifest = json.loads(path.read_text())
    entries = [table for table in manifest['tables'] if table['name'] == 'bills_versions']
    if len(entries) != 1 or entries[0]['file'] != 'bills_versions.csv.gz':
        raise ValueError('invalid version inventory manifest')
    entry = entries[0]
    source = path.parent / entry['file']
    if source.is_symlink() or source.stat().st_size != entry['bytes'] or entry['bytes'] > 1_000_000_000:
        raise ValueError('unsafe or oversized inventory source')
    with source.open('rb') as stream:
        sha = hashlib.file_digest(stream, 'sha256').hexdigest()
    if sha != entry['sha256']:
        raise ValueError('inventory source hash mismatch')
    selected = []
    count = 0
    csv.field_size_limit(sys.maxsize)
    with gzip.open(source, 'rt', newline='') as stream:
        for count, row in enumerate(csv.DictReader(stream), 1):
            if row['bill_id'] in BILLS:
                # Only identity/metadata is read into the report; prose and
                # authenticated download URLs are intentionally excluded.
                selected.append({key: row[key] for key in FIELDS})
                if len(selected) > 100:
                    raise ValueError('target inventory bound exceeded')
    if count != entry['row_count']:
        raise ValueError('inventory row count mismatch')
    return {'manifest': str(path.resolve()), 'manifest_sha256': hashlib.sha256(path.read_bytes()).hexdigest(),
            'generated_at': manifest.get('generated_at'), 'table_sha256': sha, 'verified_rows': count,
            'rows': selected, 'null_note': 'CSV empty/null values unneeded for presence decisions; no text/null quality inferred'}


def local_inventory():
    with psycopg.connect(Settings.from_env(Path(__file__).resolve().parents[1]).database_url,
                         row_factory=dict_row, connect_timeout=5, application_name='bicam_readonly_bill_inventory') as connection:
        connection.execute('set transaction isolation level repeatable read, read only')
        connection.execute("set local statement_timeout='8s'")
        connection.execute("set local lock_timeout='1s'")
        connection.execute("set local work_mem='4MB'")
        connection.execute('set local max_parallel_workers_per_gather=0')
        observed = connection.execute('select now() as observed_at').fetchone()
        rows = connection.execute('select bill_id,govinfo_package_id,version_code,version_variant,issued_at,last_modified from bicam.bills_versions where bill_id=any(%s) order by bill_id,govinfo_package_id limit 101', (BILLS,)).fetchall()
        if len(rows) > 100:
            raise ValueError('local inventory bound exceeded')
        return {**observed, 'rows': rows}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--manifest', type=Path, action='append', required=True)
    parser.add_argument('--include-local', action='store_true')
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    if len(args.manifest) > 3 or args.output.exists():
        raise ValueError('at most three frozen inventories; output must be new')
    report = {'observed_at': datetime.now(timezone.utc).isoformat(), 'bill_ids': BILLS,
              'scope': 'published target inventory, not complete raw-ingestion history',
              'frozen': [frozen_inventory(path) for path in args.manifest],
              'local': local_inventory() if args.include_local else None}
    with args.output.open('x') as stream:
        json.dump(report, stream, indent=2, default=str)
        stream.write('\n')
    print(json.dumps({'frozen_inventories': len(report['frozen']), 'selected_rows': [len(x['rows']) for x in report['frozen']],
                      'local_rows': len(report['local']['rows']) if report['local'] else None}))


if __name__ == '__main__':
    main()

import csv
import gzip
import hashlib
import importlib.util
import json
from pathlib import Path

import pytest


PATH = Path(__file__).resolve().parents[1] / 'scripts/audit_bill_target_inventory.py'
SPEC = importlib.util.spec_from_file_location('bill_inventory', PATH)
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


@pytest.fixture
def inventory(tmp_path):
    path = tmp_path / 'bills_versions.csv.gz'
    with gzip.open(path, 'wt', newline='') as stream:
        writer = csv.DictWriter(stream, fieldnames=MODULE.FIELDS)
        writer.writeheader()
        writer.writerow({'bill_id': 'hr3426-119', 'govinfo_package_id': 'BILLS-119hr3426eh1s', 'version_code': 'EH'})
        writer.writerow({'bill_id': 'hr1-119', 'govinfo_package_id': 'BILLS-119hr1ih', 'version_code': 'IH'})
    manifest = tmp_path / 'manifest.json'
    manifest.write_text(json.dumps({'tables': [{'name': 'bills_versions', 'file': path.name,
        'row_count': 2, 'bytes': path.stat().st_size, 'sha256': hashlib.sha256(path.read_bytes()).hexdigest()}]}))
    return manifest


def test_inventory_is_scoped_and_verified(inventory):
    report = MODULE.frozen_inventory(inventory)
    assert report['verified_rows'] == 2
    assert len(report['rows']) == 1
    assert report['rows'][0]['govinfo_package_id'] == 'BILLS-119hr3426eh1s'


@pytest.mark.parametrize('field,value', [('row_count', 99), ('sha256', 'wrong'), ('file', '../other.csv.gz')])
def test_bad_inventory_rejected(inventory, field, value):
    manifest = json.loads(inventory.read_text())
    manifest['tables'][0][field] = value
    inventory.write_text(json.dumps(manifest))
    with pytest.raises(ValueError):
        MODULE.frozen_inventory(inventory)

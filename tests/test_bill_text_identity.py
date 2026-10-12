from pathlib import Path
import xml.etree.ElementTree as ET
import sys
import os
import csv
import hashlib

import pytest

from bicam_collection.bill_text_identity import inspect_bill_text_identity
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from scripts.fetch_html_text import extract_visible_text


FIXTURE = Path(__file__).parent / "fixtures/bill_source_identity"
URL = "https://www.congress.gov/109/bills/hres15/BILLS-109hres15ih.htm"


def test_real_contradictory_congress_source_is_not_relinked():
    text = extract_visible_text((FIXTURE / "hres15-109.contradictory.html").read_bytes())
    result = inspect_bill_text_identity(URL, text)
    assert result["status"] == "contradiction"
    assert result["expected_bill_id"] == "hres15-109"
    assert result["conflicts"] == [{"field": "body_congress", "expected": 109, "observed": 110}]


def test_real_starprint_metadata_and_document_identity():
    root = ET.parse(FIXTURE / "eh1s.mods.xml").getroot()
    def get(name):
        return root.find(f".//{{http://www.loc.gov/mods/v3}}{name}").text
    assert (get("billVersion"), get("fileSuffix"), get("starprintNumber")) == ("eh", "eh1s", "1")
    text = extract_visible_text((FIXTURE / "eh1s.html").read_bytes())
    result = inspect_bill_text_identity("https://www.govinfo.gov/content/pkg/BILLS-119hr3426eh1s/html/BILLS-119hr3426eh1s.htm", text)
    assert result["status"] == "consistent_headings"
    assert result["expected_bill_id"] == "hr3426-119"


@pytest.mark.parametrize("body,status", [
    ("109th CONGRESS\n1st Session\nH. RES. 15\nA resolution referring to the 110th Congress and H.R. 99.", "consistent_headings"),
    ("109th CONGRESS\nH. RES. 16", "contradiction"),
    ("No document headings; prior 110th CONGRESS", "unverified"),
])
def test_only_opening_explicit_identity_is_evidence(body, status):
    assert inspect_bill_text_identity(URL, body)["status"] == status


def test_unrelated_collection_not_classified_as_bill():
    assert inspect_bill_text_identity("https://www.congress.gov/109/crpt/hrpt15/foo.htm", "110th CONGRESS")["status"] == "not_applicable"


def test_verified_xml_is_same_bill_and_version_not_cross_congress():
    root = ET.parse(FIXTURE / 'hres15-109.ih.xml').getroot()
    assert root.attrib['resolution-stage'] == 'Introduced-in-House'
    assert root.find('.//congress').text == '109th CONGRESS'
    assert root.find('.//legis-num').text == 'H. RES. 15'
    assert 'National Campus Safety Awareness Month' in root.find('.//{http://purl.org/dc/elements/1.1/}title').text


def test_reviewed_exception_binds_exact_original_response():
    with (FIXTURE.parents[2] / 'dbt_project/seeds/reconciliation/bill_text_source_exceptions.csv').open() as stream:
        exception = next(csv.DictReader(stream))
    assert exception['bill_id'] == 'hres15-109'
    assert hashlib.sha256((FIXTURE / 'hres15-109.contradictory.html').read_bytes()).hexdigest() == exception['response_hash']
    assert exception['verified_alternative_url'].endswith('BILLS-109hres15ih.xml')


@pytest.mark.skipif(os.environ.get('BICAM_RUN_VERSION_REPLAY') != '1', reason='requires explicit disposable database')
def test_postgres_cached_success_conflict_backstop():
    import jinja2
    import psycopg
    from scripts.replay_bill_version_slice import validate_test_url
    url = os.environ['BICAM_TEST_DATABASE_URL']
    validate_test_url(url)
    module = jinja2.Environment().from_string((FIXTURE.parents[2] / 'dbt_project/macros/bill_text_identity.sql').read_text()).module
    expression = module.bill_text_has_congress_conflict('prose', 'bill_id')
    contradictory = extract_visible_text((FIXTURE / 'hres15-109.contradictory.html').read_bytes())
    with psycopg.connect(url) as connection:
        connection.execute('set transaction read only')
        for prose, expected in [(contradictory, True), ('109th CONGRESS\nH. RES. 15', False), ('No header', False), (None, False)]:
            actual = connection.execute(f'SELECT {expression} FROM (VALUES (%s::text,%s::text)) AS sample(prose,bill_id)', (prose, 'hres15-109')).fetchone()[0]
            assert actual == expected


@pytest.mark.skipif(os.environ.get('BICAM_RUN_VERSION_REPLAY') != '1', reason='requires explicit disposable database')
def test_actual_publication_sql_excludes_cached_conflict():
    import jinja2
    import psycopg
    from scripts.replay_bill_version_slice import validate_test_url
    url = os.environ['BICAM_TEST_DATABASE_URL']
    validate_test_url(url)
    root = FIXTURE.parents[2] / 'dbt_project'
    model = (root / 'models/marts/bicam/bills_texts.sql').read_text()
    template = (root / 'macros/bill_text_identity.sql').read_text() + model
    rendered = jinja2.Environment().from_string(template).render(config=lambda **kwargs: '', ref=lambda name: name)
    rendered = rendered.replace('public.html_text_successful_current', 'test_html')
    ctes = '''with stg_congress__bill_texts as (
        select 'hres15-109'::text as bill_id, '2005-01-04'::date as date, 'Introduced in House'::text as type,
               %s::text as raw_text, 'source.htm'::text as formatted_text, null::text as pdf, 'source.xml'::text as xml
    ), bills as (select 'hres15-109'::text as bill_id),
    test_html as (select 'source.htm'::text as url, 'success'::text as extraction_status, %s::text as text_content)
    '''
    body = extract_visible_text((FIXTURE / 'hres15-109.contradictory.html').read_bytes())
    with psycopg.connect(url) as connection:
        connection.execute('set transaction read only')
        # Legacy successful cache excluded; independent staging prose is not
        # replaced, and a fixed source can subsequently pass the header guard.
        for staged, html, expected in [(None, body, None), (body, 'correct fallback', None), ('independent prose', body, 'independent prose'),
                                       (None, '109th CONGRESS\nH. RES. 15\nCorrected', '109th CONGRESS\nH. RES. 15\nCorrected')]:
            row = connection.execute(ctes + rendered, (staged, html)).fetchone()
            assert row[0] == 'hres15-109'
            assert row[3] == expected


@pytest.mark.skipif(os.environ.get('BICAM_RUN_VERSION_REPLAY') != '1', reason='requires explicit disposable database')
def test_actual_exception_model_keeps_reviewed_and_detected_records():
    import jinja2
    import psycopg
    import json
    from scripts.replay_bill_version_slice import validate_test_url
    url = os.environ['BICAM_TEST_DATABASE_URL']
    validate_test_url(url)
    model = (FIXTURE.parents[2] / 'dbt_project/models/marts/operations/fct_bill_text_source_exceptions.sql').read_text()
    rendered = jinja2.Environment().from_string(model).render(config=lambda **kwargs: '', ref=lambda name: name)
    rendered = rendered.replace('public.html_text_versions', 'test_html')
    ctes = '''with bill_text_source_exceptions as (
      select 'hres15-109'::text as bill_id, 'source.htm'::text as source_url, 'known'::text as response_hash,
             'body_congress_mismatch'::text as reason_code, 'correct.xml'::text as verified_alternative_url,
             'verified_alternative_not_applied'::text as review_status, '2026-09-08'::date as reviewed_at
    ), test_html as (
      select 'source.htm'::text as url, hash as response_hash, 'identity_conflict'::text as extraction_status,
             %s::text as error, '2026-09-08'::timestamptz as fetched_at
      from (values ('known'), ('new-hash')) inputs(hash)
    ) '''
    with psycopg.connect(url) as connection:
        connection.execute('set transaction read only')
        rows = connection.execute(ctes + rendered, (json.dumps({'expected_bill_id': 'hres15-109'}),)).fetchall()
        assert len(rows) == 2
        assert {row[5] for row in rows} == {'verified_alternative_not_applied', 'requires_review'}

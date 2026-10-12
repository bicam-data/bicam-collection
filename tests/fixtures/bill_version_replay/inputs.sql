-- Synthetic publication-boundary inputs. Not adjudicated real-world labels.
-- This fixture deliberately includes a stale bridge/current-raw disagreement.
create table public.input_raw_versions (
    version_id text primary key, raw_id text, endpoint text,
    observed_at timestamptz, response jsonb
);
insert into public.input_raw_versions values
('g-ih-1', 'raw-ih', 'https://api.govinfo.gov/packages/BILLS-118hr1ih/summary', '2026-09-01T00:00:00Z',
 '{"packageId":"BILLS-118hr1ih","download":{"txtLink":"https://fixture.test/BILLS-118hr1ih.htm"}}'),
('g-rh-1', 'raw-rh', 'https://api.govinfo.gov/packages/BILLS-118hr1rh/summary', '2026-09-01T00:00:00Z',
 '{"packageId":"BILLS-118hr1rh","download":{"txtLink":"https://fixture.test/old/BILLS-118hr1rh.htm"}}'),
('g-rh-2', 'raw-rh', 'https://api.govinfo.gov/packages/BILLS-118hr1rh/summary', '2026-09-02T00:00:00Z',
 '{"packageId":"BILLS-118hr1rh","download":{"txtLink":"https://fixture.test/new/BILLS-118hr1rh.htm"}}'),
('g-legacy-1', 'raw-legacy', 'https://api.govinfo.gov/packages/BILLS-108hr10v1rh/summary', '2026-09-01T00:00:00Z',
 '{"packageId":"BILLS-108hr10v1rh","download":{"txtLink":"https://fixture.test/BILLS-108hr10v1rh.htm"}}'),
('c-1', 'raw-congress', 'https://api.congress.gov/v3/bill/118/hr/1', '2026-09-01T00:00:00Z',
 '{"bill":{"congress":118,"type":"HR","number":"1","title":"Synthetic bill"}}');

create table public.input_bridge (
    canonical_bill_id text, package_id text primary key, bill_version text,
    package_title text, package_short_title text, issued_at date,
    last_modified timestamptz, is_representative_package boolean,
    govinfo_raw_id text, govinfo_raw_response_version_id text,
    congress_raw_response_version_id text
);
insert into public.input_bridge values
('hr1-118', 'BILLS-118hr1ih', 'ih', 'Synthetic introduced title', null, '2023-01-01', '2026-09-01T00:00:00Z', false, 'raw-ih', 'g-ih-1', 'c-1'),
('hr1-118', 'BILLS-118hr1rh', 'rh', 'Synthetic reported title', 'Short title', '2023-02-01', '2026-09-01T00:00:00Z', true, 'raw-rh', 'g-rh-1', 'c-1'),
('hr10-108', 'BILLS-108hr10v1rh', '1', 'Synthetic legacy title', null, '2003-02-01', '2026-09-01T00:00:00Z', true, 'raw-legacy', 'g-legacy-1', null),
('hr2-118', 'BILLS-118hr2ih', 'ih', 'Missing historical version', null, '2023-01-01', '2026-09-01T00:00:00Z', true, 'raw-missing', 'missing-version', null),
('hr3-118', 'BILLS-118hr3ih', 'ih', 'Missing bridge version ID', null, '2023-01-01', '2026-09-01T00:00:00Z', true, 'raw-null', null, null),
('hr4-118', 'BILLS-118hr4ih', 'ih', 'Wrong raw-record owner', null, '2023-01-01', '2026-09-01T00:00:00Z', true, 'raw-wrong-owner', 'g-ih-1', null);

-- Real metadata/rendition identity from ../bill_source_identity/eh1s.mods.xml.
-- The normalized boundary below is fixture data, not a production capture.
insert into public.input_raw_versions values
('g-star-1', 'raw-star', 'https://api.govinfo.gov/packages/BILLS-119hr3426eh1s/summary', '2026-09-08T00:00:00Z',
 '{"packageId":"BILLS-119hr3426eh1s","billVersion":"eh","download":{"txtLink":"https://www.govinfo.gov/content/pkg/BILLS-119hr3426eh1s/html/BILLS-119hr3426eh1s.htm"}}');
insert into public.input_bridge values
('hr3426-119', 'BILLS-119hr3426eh1s', 'eh', 'Courthouse Affordability and Space Efficiency Act of 2025', null,
 '2025-09-15', '2026-07-10T22:07:07Z', true, 'raw-star', 'g-star-1', null);

create table public.input_current as
select raw_id, version_id as raw_response_version_id, response
from public.input_raw_versions where version_id <> 'g-rh-1';
insert into public.input_current values
('raw-missing', 'newer-version', '{"download":{"txtLink":"https://fixture.test/newer.htm"}}'),
('raw-null', 'newer-version', '{"download":{"txtLink":"https://fixture.test/newer.htm"}}'),
('raw-wrong-owner', 'newer-version', '{"download":{"txtLink":"https://fixture.test/newer.htm"}}');
create view public.raw_response_versions as
select version_id as id, raw_id as raw_response_id, response from public.input_raw_versions;
create table public.html_text_successful_current (
    id text primary key, url text unique, fetched_at timestamptz,
    extraction_status text, text_content text
);
insert into public.html_text_successful_current values
('html-ih', 'https://fixture.test/BILLS-118hr1ih.htm', '2026-09-01T00:00:00Z', 'success', E'Synthetic introduced text.\nSection 2. café'),
('html-rh-old', 'https://fixture.test/old/BILLS-118hr1rh.htm', '2026-09-01T00:00:00Z', 'success', 'Synthetic original text.'),
('html-rh', 'https://fixture.test/new/BILLS-118hr1rh.htm', '2026-09-02T00:00:00Z', 'success', 'Synthetic revised text.');

create table public.input_titles (
    bill_id text, title_type text, title text,
    bill_text_version_code text, bill_text_version_name text
);
insert into public.input_titles values
('hr1-118', 'official', 'Synthetic introduced title', 'IH', 'Introduced in House'),
('hr1-118', 'official', 'Synthetic reported title', 'RH', 'Reported in House'),
('hr1-118', 'official', 'Synthetic missing package', 'ENR', 'Enrolled'),
('hr1-118', 'other', 'Ambiguous label A', 'IH', 'Shared ambiguous label'),
('hr1-118', 'other', 'Ambiguous label B', 'RH', 'Shared ambiguous label');
create table public.input_texts (
    bill_id text, date date, type text, formatted_text text, xml text, pdf text
);
insert into public.input_texts values
('hr1-118', '2023-01-01', 'Introduced in House', 'https://fixture.test/BILLS-118hr1ih.htm', null, null),
('hr1-118', '2023-02-01', 'Reported in House', null, null, null),
('hr1-118', '2023-03-01', 'Enrolled', 'https://fixture.test/BILLS-118hr1enr.htm', null, null),
('hr1-118', '2023-04-01', 'Shared ambiguous label', null, null, null),
('hr1-118', '2023-05-01', 'Numeric variant', 'https://fixture.test/BILLS-118hr1rh2.xml?download=1#text', null, null),
('hr1-118', '2023-06-01', 'PDF fallback', 'https://fixture.test/other.htm', null, 'https://fixture.test/BILLS-118hr1ih.pdf'),
('hr1-118', '2023-07-01', 'Unrelated URL', 'https://fixture.test/other.htm?link=/BILLS-118hr1ih.pdf', null, null),
('hr10-108', '2003-02-01', 'Legacy variant', 'https://fixture.test/BILLS-108hr10v1rh.htm', null, null);
insert into public.input_texts values
('hr3426-119', '2025-09-15', 'Engrossed in House',
 'https://www.congress.gov/119/bills/hr3426/BILLS-119hr3426eh1s.htm',
 'https://www.congress.gov/119/bills/hr3426/BILLS-119hr3426eh1s.xml',
 'https://www.congress.gov/119/bills/hr3426/BILLS-119hr3426eh1s.pdf');
create table public.input_actions (action_id text primary key, bill_id text, text text);
insert into public.input_actions values
('a-exact', 'hr1-118', 'Reported in House'),
('a-fuzzy', 'hr1-118', 'Reported in House with changes'),
('a-ambiguous', 'hr1-118', 'Shared ambiguous label');

-- Boundary rows remain traceable individually; these ordinals are fixture
-- identities, not a proposed change to the public contract.
create table public.input_child_evidence (
    source_table text, ordinal integer, endpoint text, observed_at timestamptz,
    source_row jsonb, primary key(source_table, ordinal)
);
insert into public.input_child_evidence
select 'bills_titles', row_number() over(order by title)::integer,
       'https://api.congress.gov/v3/bill/118/hr/1/titles', '2026-09-01T00:00:00Z', to_jsonb(t)
from public.input_titles t;
insert into public.input_child_evidence
select 'bills_texts', row_number() over(order by bill_id, date)::integer,
       'https://api.congress.gov/v3/bill/' || split_part(bill_id,'-',2) || '/hr/' || substring(split_part(bill_id,'-',1) from 3) || '/text',
       '2026-09-01T00:00:00Z', to_jsonb(t) from public.input_texts t;
insert into public.input_child_evidence
select 'bills_actions', row_number() over(order by action_id)::integer,
       'https://api.congress.gov/v3/bill/118/hr/1/actions', '2026-09-01T00:00:00Z', to_jsonb(t)
from public.input_actions t;

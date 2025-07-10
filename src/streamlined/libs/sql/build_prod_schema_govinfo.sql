CREATE SCHEMA IF NOT EXISTS bicam_govinfo;

BEGIN;

SET CONSTRAINTS ALL DEFERRED;

CREATE TABLE IF NOT EXISTS bicam_govinfo.bills(
    package_id TEXT PRIMARY KEY,
    bill_id TEXT,
    bill_version TEXT,
    origin_chamber TEXT, -- needs to be lowered
    current_chamber TEXT, -- needs to be lowered
    is_appropriation BOOLEAN,
    is_private BOOLEAN,
    pages INTEGER,
    issued_at DATE,
    government_author1 TEXT,
    government_author2 TEXT,
    publisher TEXT,
    collection_code TEXT,
    stock_number TEXT,
    su_doc_class_number TEXT,
    migrated_doc_id TEXT,
    child_ils_system_id TEXT,
    parent_ils_system_id TEXT,
    mods_url TEXT,
    pdf_url TEXT,
    premis_url TEXT,
    txt_url TEXT,
    xml_url TEXT,
    zip_url TEXT,
    last_modified TIMESTAMP WITH TIME ZONE
);

-- Create table for legislative document versions
CREATE TABLE IF NOT EXISTS bicam_govinfo.ref_bill_version_codes (
    version_code TEXT PRIMARY KEY,
    description TEXT
);

-- Insert data
INSERT INTO bicam_govinfo.ref_bill_version_codes (version_code, description) VALUES
    ('AS', 'Additional sponsors'),
    ('ASH', 'Agreed to in Senate, with an amendment'),
    ('EAS', 'Engrossed amendment, Senate'),
    ('EH', 'Engrossed in House'),
    ('ENR', 'Enrolled bill'),
    ('ES', 'Engrossed in Senate'),
    ('FES', 'Further revised engrossed in Senate'),
    ('HE', 'Held at desk'),
    ('HES', 'Held at desk in Senate'),
    ('IHS', 'Identical bill laid on table in House'),
    ('IH', 'Introduced in House'),
    ('IS', 'Introduced in Senate'),
    ('LTS', 'Laid on table in Senate'),
    ('PCS', 'Placed on calendar in Senate'),
    ('PP', 'Public print'),
    ('PAP', 'Passed/agreed to in House'),
    ('PAS', 'Passed/agreed to in Senate'),
    ('RCS', 'Received in Senate'),
    ('RDH', 'Received in House'),
    ('REH', 'Reference to committee in House'),
    ('REN', 'Re-engrossed'),
    ('RES', 'Referred in Senate'),
    ('RFH', 'Referred in House'),
    ('RFS', 'Referred to committee in Senate'),
    ('RH', 'Reported in House'),
    ('RIS', 'Referred to committee in Senate with instructions'),
    ('RS', 'Reported in Senate'),
    ('SC', 'Sponsor change')
ON CONFLICT (version_code) DO NOTHING;

GRANT SELECT ON ALL TABLES IN SCHEMA bicam_govinfo TO bicam_pipeline;

CREATE TABLE IF NOT EXISTS bicam_govinfo.bills_reference_codes(
    bill_code_id TEXT PRIMARY KEY,
    package_id TEXT,
    reference_code TEXT -- combination of label and title
);

CREATE TABLE IF NOT EXISTS bicam_govinfo.bills_reference_codes_sections(
    bill_code_id TEXT,
    code_section TEXT,
    UNIQUE (bill_code_id, code_section)
);

CREATE TABLE IF NOT EXISTS bicam_govinfo.bills_reference_laws(
    package_id TEXT,
    law_id TEXT, -- fix law_id
    law_type TEXT,
    PRIMARY KEY (package_id, law_id)
);

CREATE TABLE IF NOT EXISTS bicam_govinfo.bills_reference_statutes(
    bill_statute_id TEXT PRIMARY KEY,
    package_id TEXT,
    reference_statute TEXT -- combine label and title
);

CREATE TABLE IF NOT EXISTS bicam_govinfo.bills_reference_statutes_pages(
    bill_statute_id TEXT,
    page TEXT, -- combine label and title
    UNIQUE (bill_statute_id, page)
);

CREATE TABLE IF NOT EXISTS bicam_govinfo.bills_short_titles(
    package_id TEXT,
    short_title TEXT,
    level TEXT,
    type TEXT,
    UNIQUE (package_id, short_title, level, type)
);

CREATE TABLE IF NOT EXISTS bicam_govinfo.committeeprints(
    package_id TEXT PRIMARY KEY,
    print_id TEXT,
    title TEXT,
    chamber TEXT, -- lower
    congress INTEGER,
    session INTEGER,
    pages INTEGER,
    document_number TEXT,
    issued_at DATE,
    branch TEXT,
    government_author1 TEXT,
    government_author2 TEXT,
    publisher TEXT,
    collection_code TEXT,
    migrated_doc_id TEXT,
    su_doc_class_number TEXT,
    last_modified TIMESTAMP WITH TIME ZONE
);

CREATE TABLE IF NOT EXISTS bicam_govinfo.committeeprints_granules(
    granule_id TEXT,
    package_id TEXT,
    PRIMARY KEY (granule_id, package_id)
);

CREATE TABLE IF NOT EXISTS bicam_govinfo.committeeprints_committees(
    package_id TEXT,
    granule_id TEXT,
    committee_code TEXT,
    committee_name TEXT,
    chamber TEXT,
    UNIQUE (package_id, granule_id, committee_code, committee_name, chamber)
);

CREATE TABLE IF NOT EXISTS bicam_govinfo.committeeprints_reference_bills(
    package_id TEXT,
    granule_id TEXT,
    bill_id TEXT,
    UNIQUE (package_id, granule_id, bill_id)
);

CREATE TABLE IF NOT EXISTS bicam_govinfo.committeereports(
    package_id TEXT PRIMARY KEY,
    report_id TEXT,
    title TEXT,
    subtitle TEXT,
    chamber TEXT, -- lower
    congress INTEGER,
    session INTEGER,
    pages INTEGER,
    issued_at DATE,
    branch TEXT,
    government_author1 TEXT,
    government_author2 TEXT,
    publisher TEXT,
    collection_code TEXT,
    migrated_doc_id TEXT,
    su_doc_class_number TEXT,
    last_modified TIMESTAMP WITH TIME ZONE
);

-- SERIAL DETAILS/SUBJECTS????

CREATE TABLE IF NOT EXISTS bicam_govinfo.committeereports_granules(
    granule_id TEXT,
    package_id TEXT,
    PRIMARY KEY (granule_id, package_id)
);

CREATE TABLE IF NOT EXISTS bicam_govinfo.committeereports_committees(
    granule_id TEXT,
    package_id TEXT,
    committee_code TEXT,
    committee_name TEXT,
    chamber TEXT,
    UNIQUE (package_id, granule_id, committee_code, committee_name, chamber)
);

CREATE TABLE IF NOT EXISTS bicam_govinfo.committeereports_members(
    granule_id TEXT,
    package_id TEXT,
    bioguide_id TEXT,
    UNIQUE (package_id, granule_id, bioguide_id)

);

CREATE TABLE IF NOT EXISTS bicam_govinfo.committeereports_reference_bills(
    granule_id TEXT,
    package_id TEXT,
    bill_id TEXT,
    UNIQUE (package_id, granule_id, bill_id)
);


CREATE TABLE IF NOT EXISTS bicam_govinfo.hearings(
    package_id TEXT PRIMARY KEY,
    hearing_id TEXT,
    title TEXT,
    chamber TEXT, -- lower
    congress INTEGER,
    session INTEGER,
    pages INTEGER,
    is_appropriation BOOLEAN,
    issued_at DATE,
    branch TEXT,
    government_author1 TEXT,
    government_author2 TEXT,
    publisher TEXT,
    collection_code TEXT,
    migrated_doc_id TEXT,
    su_doc_class_number TEXT,
    last_modified TIMESTAMP WITH TIME ZONE
);

-- DATES???

CREATE TABLE IF NOT EXISTS bicam_govinfo.hearings_granules(
    granule_id TEXT,
    package_id TEXT,
    PRIMARY KEY (granule_id, package_id)
);

CREATE TABLE IF NOT EXISTS bicam_govinfo.hearings_committees(
    granule_id TEXT,
    package_id TEXT,
    committee_code TEXT,
    committee_name TEXT,
    UNIQUE (package_id, granule_id, committee_code, committee_name)
);

CREATE TABLE IF NOT EXISTS bicam_govinfo.hearings_members(
    granule_id TEXT,
    package_id TEXT,
    bioguide_id TEXT,
    name TEXT,
    UNIQUE (package_id, granule_id, bioguide_id, name)
);

CREATE TABLE IF NOT EXISTS bicam_govinfo.hearings_reference_bills(
    granule_id TEXT,
    package_id TEXT,
    bill_id TEXT,
    UNIQUE (package_id, granule_id, bill_id)
);

CREATE TABLE IF NOT EXISTS bicam_govinfo.hearings_witnesses(
    granule_id TEXT,
    witness TEXT,
    UNIQUE (granule_id, witness)
);

CREATE TABLE IF NOT EXISTS bicam_govinfo.treaties(
    package_id TEXT PRIMARY KEY,
    treaty_id TEXT,
    title TEXT,
    congress INTEGER,
    session INTEGER,
    chamber TEXT, -- lower
    summary TEXT,
    pages INTEGER,
    issued_at DATE,
    branch TEXT,
    government_author1 TEXT,
    government_author2 TEXT,
    publisher TEXT,
    collection_code TEXT,
    migrated_doc_id TEXT,
    su_doc_class_number TEXT,
    last_modified TIMESTAMP WITH TIME ZONE
);

CREATE TABLE IF NOT EXISTS bicam_govinfo.treaties_granules(
    granule_id TEXT,
    package_id TEXT,
    PRIMARY KEY (granule_id, package_id)
);

CREATE TABLE IF NOT EXISTS bicam_govinfo.treaties_committees(
    granule_id TEXT,
    package_id TEXT,
    committee_code TEXT,
    committee_name TEXT,
    chamber TEXT,
    UNIQUE (package_id, granule_id, committee_code, committee_name, chamber)
);

CREATE TABLE IF NOT EXISTS bicam_govinfo.congressional_directories(
    package_id TEXT PRIMARY KEY,
    title TEXT,
    congress INTEGER,
    issued_at TIMESTAMP WITH TIME ZONE,
    branch TEXT,
    government_author1 TEXT,
    government_author2 TEXT,
    publisher TEXT,
    collection_code TEXT,
    ils_system_id TEXT,
    migrated_doc_id TEXT,
    su_doc_class_number TEXT,
    text_url TEXT,
    pdf_url TEXT,
    last_modified TIMESTAMP WITH TIME ZONE
);

CREATE TABLE IF NOT EXISTS bicam_govinfo.congressional_directories_isbn(
    package_id TEXT,
    isbn TEXT,
    UNIQUE(package_id, isbn)
);

CREATE TABLE IF NOT EXISTS bicam_govinfo.members(
    granule_id TEXT,
    package_id TEXT,
    bioguide_id TEXT,
    membername TEXT,
    title TEXT,
    biography TEXT,
    member_type TEXT,
    chamber TEXT,
    population INTEGER,
    gpo_id TEXT,
    authority_id TEXT,
    email_address TEXT,
    official_url TEXT,
    twitter_url TEXT,
    instagram_url TEXT,
    facebook_url TEXT,
    youtube_url TEXT,
    last_modified TIMESTAMP WITH TIME ZONE,
    PRIMARY KEY (granule_id, package_id)
);

CREATE TABLE IF NOT EXISTS bicam_govinfo.members_zipcodes(
    package_id TEXT,
    granule_id TEXT,
    bioguide_id TEXT,
    zipcode TEXT,
    UNIQUE (package_id, granule_id, bioguide_id, zipcode)
);

-- Finish off with granting permissions
DO $$
DECLARE
    current_table_name text;
    current_schema_name text;
BEGIN
    FOR current_schema_name IN SELECT schema_name FROM information_schema.schemata WHERE schema_name IN ('bicam_staging_govinfo', 'bicam_govinfo') LOOP
        FOR current_table_name IN SELECT table_name FROM information_schema.tables WHERE table_schema = current_schema_name LOOP
            EXECUTE format('GRANT ALL ON TABLE %I.%I TO bicam_pipeline', current_schema_name, current_table_name);
        END LOOP;
    END LOOP;
END$$;

END TRANSACTION;

COMMIT;

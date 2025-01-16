BEGIN TRANSACTION;

-- Create the new schema
CREATE SCHEMA IF NOT EXISTS _staging_govinfo;

-- Bills table
CREATE TABLE IF NOT EXISTS _staging_govinfo.bills (
    bill_id TEXT NOT NULL
    package_id TEXT,  -- GovInfo specific
    su_doc_class_number TEXT,

    collection_code TEXT,
    collection_name TEXT,
    category TEXT,
    doc_class TEXT,
    bill_version TEXT,
    origin_chamber TEXT,
    current_chamber TEXT,
    is_private BOOLEAN,
    is_appropriation BOOLEAN,
    pages INTEGER,
    government_author1 TEXT,
    government_author2 TEXT,
    publisher TEXT,
    date_issued TIMESTAMP WITH TIME ZONE,
    migrated_doc_id TEXT,
    parent_ils_system_id TEXT,
    child_ils_title TEXT,
    parent_ils_title TEXT,
    child_ils_system_id TEXT,
    stock_number TEXT,
    pdf_url TEXT,  -- GovInfo specific
    txt_url TEXT,  -- GovInfo specific
    xml_url TEXT,  -- GovInfo specific
    mods_url TEXT,  -- GovInfo specific
    zip_link TEXT,
    last_modified TIMESTAMP WITH TIME ZONE,
    _inserted_at TIMESTAMP WITH TIME ZONE
    UNIQUE ()
);

CREATE TABLE IF NOT EXISTS _staging_govinfo.references (
    reference_id SERIAL PRIMARY KEY,
    collection_name TEXT,
    collection_code TEXT,
    package_id TEXT,
    UNIQUE (collection_name, collection_code, package_id)
);

-- Create the reference_codes table
CREATE TABLE IF NOT EXISTS _staging_govinfo.reference_codes (
    code_id SERIAL PRIMARY KEY,
    reference_id INTEGER REFERENCES _staging_govinfo.references(reference_id),
    title TEXT,
    label TEXT
);

-- Create the reference_code_sections table
CREATE TABLE IF NOT EXISTS _staging_govinfo.reference_code_sections (
    section_id SERIAL PRIMARY KEY,
    code_id INTEGER REFERENCES _staging_govinfo.reference_codes(code_id),
    section TEXT
);

-- Create the reference_statutes table
CREATE TABLE IF NOT EXISTS _staging_govinfo.reference_statutes (
    statute_id SERIAL PRIMARY KEY,
    reference_id INTEGER REFERENCES _staging_govinfo.references(reference_id),
    title TEXT,
    label TEXT
);

-- Create the reference_statute_pages table
CREATE TABLE IF NOT EXISTS _staging_govinfo.reference_statute_pages (
    page_id SERIAL PRIMARY KEY,
    statute_id INTEGER REFERENCES _staging_govinfo.reference_statutes(statute_id),
    page INTEGER
);

-- Create the reference_laws table
CREATE TABLE IF NOT EXISTS _staging_govinfo.reference_laws (
    law_id TEXT,
    reference_id INTEGER REFERENCES _staging_govinfo.references(reference_id),
    label TEXT,
    congress INTEGER,
    number INTEGER
);

-- Create the reference_bills table
CREATE TABLE IF NOT EXISTS _staging_govinfo.reference_bills (
    bill_id TEXT REFERENCES _staging_govinfo.bills(bill_id),
    reference_id INTEGER REFERENCES _staging_govinfo.references(reference_id),
    number INTEGER,
    congress INTEGER,
    type TEXT
);

--Congressional Directory table
CREATE TABLE IF NOT EXISTS _staging_govinfo.congressional_directory (
    package_id TEXT,
    su_doc_class_number TEXT,
    collection_code TEXT,
    collection_name TEXT,
    doc_class TEXT,
    document_type TEXT,
    category TEXT,
    branch TEXT,
    title TEXT,
    congress TEXT,
    government_author1 TEXT,
    government_author2 TEXT,
    publisher TEXT,
    date_issued TIMESTAMP WITH TIME ZONE,
    migrated_doc_id TEXT,
    ils_system_id TEXT,
    isbn TEXT,
    premis_url TEXT,
    zip_url TEXT,
    mods_url TEXT,
    pdf_url TEXT,
    txt_url TEXT,
    xml_url TEXT,
    last_modified TIMESTAMP WITH TIME ZONE,
    _inserted_at TIMESTAMP WITH TIME ZONE
);

CREATE TABLE IF NOT EXISTS _staging_govinfo.congressional_directory_members (
    bioguide_id TEXT,
    package_id TEXT,
    granule_id TEXT,
    title TEXT,
    biography TEXT,
    gpo_id TEXT,
    authority_id TEXT,
    population TEXT,
    granule_class TEXT,
    sub_granule_class TEXT,
    date_issued TIMESTAMP WITH TIME ZONE,
    official_url TEXT,
    facebook_url TEXT,
    twitter_url TEXT,
    youtube_url TEXT,
    instagram_url TEXT,
    other_url TEXT,
    last_modified TIMESTAMP WITH TIME ZONE,
    _inserted_at TIMESTAMP WITH TIME ZONE
    UNIQUE (bioguide_id, package_id)
)


-- Congressional Reports table
CREATE TABLE IF NOT EXISTS _staging_govinfo.congressional_reports (
    report_id TEXT,
    package_id TEXT,
    su_doc_class_number TEXT,
    title TEXT,
    subtitle TEXT,
    pages INTEGER,
    congress TEXT,
    session TEXT,
    chamber TEXT,
    collection_code TEXT,
    collection_name TEXT,
    doc_class TEXT,
    document_type TEXT,
    category TEXT,
    branch TEXT,
    government_author1 TEXT,
    government_author2 TEXT,
    publisher TEXT,
    date_issued TIMESTAMP WITH TIME ZONE,
    migrated_doc_id TEXT,
    premis_url TEXT,
    zip_url TEXT,
    pdf_url TEXT,  -- GovInfo specific
    txt_url TEXT,  -- GovInfo specific
    xml_url TEXT,  -- GovInfo specific
    mods_url TEXT,  -- GovInfo specific
    last_modified TIMESTAMP WITH TIME ZONE,
    _inserted_at TIMESTAMP WITH TIME ZONE
);

CREATE TABLE IF NOT EXISTS _staging_govinfo.congressional_reports_parts (
    package_id TEXT,
    granule_id TEXT,
    title TEXT,
    collection_code TEXT,
    collection_name TEXT,
    doc_class TEXT,
    granule_class TEXT,
    category TEXT,
    date_issued TIMESTAMP WITH TIME ZONE,
    premis_url TEXT,
    zip_url TEXT,
    pdf_url TEXT,
    txt_url TEXT,
    mods_url TEXT,
    last_modified TIMESTAMP WITH TIME ZONE,
    _inserted_at TIMESTAMP WITH TIME ZONE
);

CREATE TABLE IF NOT EXISTS _staging_govinfo.reports_references (
    reference_id SERIAL PRIMARY KEY,
    collection_name TEXT,
    collection_code TEXT,
    package_id TEXT REFERENCES _staging_govinfo.congressional_reports(package_id),
    granule_id TEXT REFERENCES _staging_govinfo.congressional_reports_parts(granule_id),
    UNIQUE (collection_name, collection_code, package_id, granule_id)
);


-- Create the reference_bills table
CREATE TABLE IF NOT EXISTS _staging_govinfo.reports_references_bills(
    package_id TEXT REFERENCES _staging_govinfo.congressional_reports(package_id),
    granule_id TEXT REFERENCES _staging_govinfo.congressional_reports_parts(granule_id),
    reference_id INTEGER REFERENCES _staging_govinfo.references(reference_id),
    bill_id INTEGER REFERENCES _staging_govinfo.bills(bill_id),
    number INTEGER,
    congress INTEGER,
    type TEXT
);

CREATE TABLE IF NOT EXISTS _staging_govinfo.reports_member_roles(
    package_id TEXT REFERENCES _staging_govinfo.congressional_reports(package_id),
    granule_id TEXT REFERENCES _staging_govinfo.congressional_reports_parts(granule_id),
    bioguide_id TEXT REFERENCES _staging_govinfo.congressional_directory_members(bioguide_id),
    role TEXT
);


CREATE TABLE IF NOT EXISTS _staging_govinfo.treaties (
    treaty_id TEXT,
    package_id TEXT,
    su_doc_class_number TEXT,
    title TEXT,
    congress TEXT,
    session TEXT,
    chamber TEXT,
    pages INTEGER,
    collection_code TEXT,
    collection_name TEXT,
    doc_class TEXT,
    document_type TEXT,
    document_number TEXT,
    category TEXT,
    branch TEXT,
    government_author1 TEXT,
    government_author2 TEXT,
    publisher TEXT,
    date_issued TIMESTAMP WITH TIME ZONE,
    migrated_doc_id TEXT,
    ils_system_id TEXT,
    premis_url TEXT,
    zip_url TEXT,
    pdf_url TEXT,
    txt_url TEXT,
    mods_url TEXT,
    last_modified TIMESTAMP WITH TIME ZONE,
    _inserted_at TIMESTAMP WITH TIME ZONE
)

CREATE TABLE IF NOT EXISTS _staging_govinfo.treaty_parts (
    treaty_id TEXT REFERENCES _staging_govinfo.treaties(treaty_id),
    package_id TEXT REFERENCES _staging_govinfo.treaties(package_id),
    granule_id TEXT,
    title TEXT,
    summary TEXT,
    collection_code TEXT,
    collection_name TEXT,
    doc_class TEXT,
    granule_class TEXT,
    category TEXT,
    date_issued TIMESTAMP WITH TIME ZONE,
    is_graphics_in_pdf BOOLEAN,
    premis_url TEXT,
    zip_url TEXT,
    pdf_url TEXT,
    txt_url TEXT,
    mods_url TEXT,
    last_modified TIMESTAMP WITH TIME ZONE,
    _inserted_at TIMESTAMP WITH TIME ZONE
)

CREATE TABLE IF NOT EXISTS _staging_govinfo.committee_prints (
    print_id TEXT,
    package_id TEXT,
    su_doc_class_number TEXT,
    title TEXT,
    congress TEXT,
    session TEXT,
    chamber TEXT,
    pages INTEGER,
    collection_code TEXT,
    collection_name TEXT,
    doc_class TEXT,
    document_type TEXT,
    document_number TEXT,
    category TEXT,
    branch TEXT,
    government_author1 TEXT,
    government_author2 TEXT,
    publisher TEXT,
    date_issued TIMESTAMP WITH TIME ZONE,
    migrated_doc_id TEXT,
    premis_url TEXT,
    zip_url TEXT,
    pdf_url TEXT,
    txt_url TEXT,
    mods_url TEXT,
    last_modified TIMESTAMP WITH TIME ZONE,
    _inserted_at TIMESTAMP WITH TIME ZONE
)

CREATE TABLE IF NOT EXISTS _staging_govinfo.committee_prints_parts (
    print_id TEXT REFERENCES _staging_govinfo.committee_prints(print_id),
    package_id TEXT REFERENCES _staging_govinfo.committee_prints(package_id),
    granule_id TEXT,
    title TEXT,
    collection_code TEXT,
    collection_name TEXT,
    doc_class TEXT,
    granule_class TEXT,
    category TEXT,
    date_issued TIMESTAMP WITH TIME ZONE,
    is_graphics_in_pdf BOOLEAN,
    premis_url TEXT,
    zip_url TEXT,
    pdf_url TEXT,
    txt_url TEXT,
    mods_url TEXT,
    granule_class TEXT,
)

CREATE TABLE IF NOT EXISTS _staging_govinfo.committee_prints_parts_references (
    print_id TEXT REFERENCES _staging_govinfo.committee_prints(print_id),
    granule_id TEXT REFERENCES _staging_govinfo.committee_prints_parts(granule_id),
    reference_id SERIAL PRIMARY KEY,
    collection_name TEXT,
    collection_code TEXT,
    UNIQUE (collection_name, collection_code, granule_id)
)


CREATE TABLE IF NOT EXISTS _staging_govinfo.committee_prints_parts_references_bills (
    print_id TEXT REFERENCES _staging_govinfo.committee_prints(print_id),
    granule_id TEXT REFERENCES _staging_govinfo.committee_prints_parts(granule_id),
    reference_id INTEGER REFERENCES _staging_govinfo.committee_prints_parts_references(reference_id),
    bill_id TEXT REFERENCES _staging_govinfo.bills(bill_id),
    number INTEGER,
    congress INTEGER,
    type TEXT
)

CREATE TABLE IF NOT EXISTS _staging_govinfo.committee_hearings (
    hearing_id TEXT,
    package_id TEXT,
    su_doc_class_number TEXT,
    title TEXT,
    congress TEXT,
    session TEXT,
    chamber TEXT,
    pages INTEGER,
    collection_code TEXT,
    collection_name TEXT,
    doc_class TEXT,
    document_type TEXT,
    document_number TEXT,
    category TEXT,
    branch TEXT,
    government_author1 TEXT,
    government_author2 TEXT,
    publisher TEXT,
    date_issued TIMESTAMP WITH TIME ZONE,
    migrated_doc_id TEXT,
    premis_url TEXT,
    zip_url TEXT,
    pdf_url TEXT,
    txt_url TEXT,
    mods_url TEXT,
    last_modified TIMESTAMP WITH TIME ZONE,
    _inserted_at TIMESTAMP WITH TIME ZONE
)


-- Hearings table
CREATE TABLE IF NOT EXISTS govinfo.hearings (
    jacketnumber TEXT PRIMARY KEY,
    package_id TEXT,
    su_doc_class_number TEXT,
    title TEXT,
    congress TEXT,
    session TEXT,
    chamber TEXT,
    pages INTEGER,
    collection_code TEXT,
    collection_name TEXT,
    doc_class TEXT,
    document_type TEXT,
    document_number TEXT,
    category TEXT,
    branch TEXT,
    government_author1 TEXT,
    government_author2 TEXT,
    publisher TEXT,
    date_issued TIMESTAMP WITH TIME ZONE,
    migrated_doc_id TEXT,
    premis_url TEXT,
    zip_url TEXT,
    pdf_url TEXT,
    txt_url TEXT,
    mods_url TEXT,
    last_modified TIMESTAMP WITH TIME ZONE,
    _inserted_at TIMESTAMP WITH TIME ZONE
);

CREATE TABLE IF NOT EXISTS _staging_govinfo.hearings_parts (
    jacketnumber TEXT REFERENCES _staging_govinfo.hearings(jacketnumber),
    package_id TEXT REFERENCES _staging_govinfo.hearings(package_id),
    granule_id TEXT,
    title TEXT,
    is_appropriation BOOLEAN,
    collection_code TEXT,
    collection_name TEXT,
    doc_class TEXT,
    granule_class TEXT,
    category TEXT,
    date_issued TIMESTAMP WITH TIME ZONE,
    is_graphics_in_pdf BOOLEAN,
    premis_url TEXT,
    zip_url TEXT,
    pdf_url TEXT,
    txt_url TEXT,
    mods_url TEXT,
    last_modified TIMESTAMP WITH TIME ZONE,
    _inserted_at TIMESTAMP WITH TIME ZONE
);

CREATE TABLE IF NOT EXISTS _staging_govinfo.hearings_parts_references (
    jacketnumber TEXT REFERENCES _staging_govinfo.hearings(jacketnumber),
    granule_id TEXT REFERENCES _staging_govinfo.hearings_parts(granule_id),
    reference_id SERIAL PRIMARY KEY,
    collection_name TEXT,
    collection_code TEXT,
    UNIQUE (collection_name, collection_code, granule_id)
);

CREATE TABLE IF NOT EXISTS _staging_govinfo.hearings_parts_references_bills (
    jacketnumber TEXT REFERENCES _staging_govinfo.hearings(jacketnumber),
    granule_id TEXT REFERENCES _staging_govinfo.hearings_parts(granule_id),
    reference_id INTEGER REFERENCES _staging_govinfo.hearings_parts_references(reference_id),
    bill_id TEXT REFERENCES _staging_govinfo.bills(bill_id),
    number INTEGER,
    congress INTEGER,
    type TEXT
);

CREATE TABLE IF NOT EXISTS _staging_govinfo.hearings_witnesses (
    jacketnumber TEXT REFERENCES _staging_govinfo.hearings(jacketnumber),
    granule_id TEXT REFERENCES _staging_govinfo.hearings_parts(granule_id),
    witness TEXT
)
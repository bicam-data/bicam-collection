-- PROD TABLES
CREATE SCHEMA IF NOT EXISTS bicam_congressional;

BEGIN;

SET CONSTRAINTS ALL DEFERRED;

-- 1. Foundational tables
CREATE TABLE IF NOT EXISTS bicam_congressional.congresses(
    congress_number INTEGER PRIMARY KEY,
    name TEXT,
    start_year INTEGER,
    end_year INTEGER,
    updated_at TIMESTAMP WITH TIME ZONE
);

CREATE TABLE IF NOT EXISTS bicam_congressional.congresses_sessions(
    congress_number INTEGER,
    session INTEGER,
    chamber TEXT,
    type TEXT,
    start_date DATE,
    end_date DATE,
    FOREIGN KEY (congress_number) REFERENCES bicam_congressional.congresses (congress_number)
        DEFERRABLE INITIALLY DEFERRED,
    UNIQUE (congress_number, session, chamber, type)
);

-- Reference tables with no dependencies
CREATE TABLE IF NOT EXISTS ref_bill_summary_version_codes (
    version_code TEXT PRIMARY KEY,
    description TEXT
);

INSERT INTO ref_bill_summary_version_codes (version_code, description) VALUES
    ('BILLS', 'Bills'),
    ('H.CON.RES.', 'House Concurrent Resolution'),
    ('H.J.RES.', 'House Joint Resolution'),
    ('H.RES.', 'House Resolution'),
    ('S.CON.RES.', 'Senate Concurrent Resolution'),
    ('S.J.RES.', 'Senate Joint Resolution'),
    ('S.RES.', 'Senate Resolution')
ON CONFLICT (version_code) DO NOTHING;

CREATE TABLE IF NOT EXISTS bicam_congressional.ref_title_type_codes (
    title_type_code INTEGER PRIMARY KEY,
    description TEXT
);

INSERT INTO bicam_congressional.ref_title_type_codes (title_type_code, description)
SELECT
    title_type_code::integer,
    title_type
FROM (
    VALUES
        (1, 'Official'),
        (2, 'Short'),
        (3, 'Popular'),
        (4, 'Display')
) AS v (title_type_code, title_type)
ON CONFLICT (title_type_code) DO NOTHING;

GRANT SELECT ON ALL TABLES IN SCHEMA bicam_congressional TO bicam_pipeline;

-- 2. Base tables that have no or simple dependencies
CREATE TABLE IF NOT EXISTS bicam_congressional.members(
    bioguide_id TEXT PRIMARY KEY,
    normalized_name TEXT,
    direct_order_name TEXT,
    inverted_order_name TEXT,
    honorific_prefix TEXT,
    first_name TEXT,
    middle_name TEXT,
    last_name TEXT,
    suffix TEXT,
    nickname TEXT,
    party TEXT,
    state TEXT,
    district INTEGER,
    birth_year INTEGER,
    death_year INTEGER,
    official_url TEXT,
    office_address TEXT,
    office_city TEXT,
    office_district TEXT,
    office_zip TEXT,
    office_phone TEXT,
    sponsored_legislation_count INTEGER,
    cosponsored_legislation_count INTEGER,
    depiction_image_url TEXT,
    depiction_attribution TEXT,
    is_current_member BOOLEAN,
    updated_at TIMESTAMP WITH TIME ZONE
);

CREATE TABLE IF NOT EXISTS bicam_congressional.committees(
    committee_code TEXT PRIMARY KEY,
    name TEXT,
    chamber TEXT,
    is_subcommittee BOOLEAN,
    is_current BOOLEAN,
    bills_count INTEGER,
    reports_count INTEGER,
    nominations_count INTEGER,
    updated_at TIMESTAMP WITH TIME ZONE
);

-- 3. Tables depending on congresses
CREATE TABLE IF NOT EXISTS bicam_congressional.bills(
    bill_id TEXT PRIMARY KEY,
    bill_type TEXT,
    bill_number TEXT,
    congress INTEGER,
    title TEXT,
    origin_chamber TEXT,
    introduced_at DATE,
    constitutional_authority_statement TEXT,
    is_law BOOLEAN,
    notes TEXT,
    policy_area TEXT,
    actions_count INTEGER,
    amendments_count INTEGER,
    committees_count INTEGER,
    cosponsors_count INTEGER,
    summaries_count INTEGER,
    subjects_count INTEGER,
    titles_count INTEGER,
    texts_count INTEGER,
    updated_at TIMESTAMP WITH TIME ZONE,
    FOREIGN KEY (congress) REFERENCES bicam_congressional.congresses (congress_number)
        DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE IF NOT EXISTS bicam_congressional.hearings(
    hearing_id TEXT PRIMARY KEY,
    hearing_jacketnumber TEXT,
    loc_id TEXT,
    title TEXT,
    congress INTEGER,
    chamber TEXT,
    hearing_number INTEGER,
    part_number TEXT,
    citation TEXT,
    updated_at TIMESTAMP WITH TIME ZONE,
    FOREIGN KEY (congress) REFERENCES bicam_congressional.congresses (congress_number)
        DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE IF NOT EXISTS bicam_congressional.treaties(
    treaty_id TEXT PRIMARY KEY,
    treaty_number INTEGER,
    suffix TEXT,
    congress_received INTEGER,
    congress_considered INTEGER,
    topic TEXT,
    transmitted_at TIMESTAMP WITH TIME ZONE,
    in_force_at TIMESTAMP WITH TIME ZONE,
    resolution_text TEXT,
    parts_count INTEGER,
    actions_count INTEGER,
    old_number TEXT,
    old_number_display_name TEXT,
    updated_at TIMESTAMP WITH TIME ZONE,
    FOREIGN KEY (congress_received) REFERENCES bicam_congressional.congresses (congress_number)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (congress_considered) REFERENCES bicam_congressional.congresses (congress_number)
        DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE IF NOT EXISTS bicam_congressional.nominations(
    nomination_id TEXT PRIMARY KEY,
    nomination_number TEXT,
    part_number TEXT,
    congress INTEGER,
    description TEXT,
    is_privileged BOOLEAN,
    is_civilian BOOLEAN,
    received_at DATE,
    authority_date DATE,
    executive_calendar_number TEXT,
    citation TEXT,
    committees_count INTEGER,
    actions_count INTEGER,
    updated_at TIMESTAMP WITH TIME ZONE,
    FOREIGN KEY (congress) REFERENCES bicam_congressional.congresses (congress_number)
        DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE IF NOT EXISTS bicam_congressional.amendments(
    amendment_id TEXT PRIMARY KEY,
    amendment_type TEXT,
    amendment_number TEXT,
    congress INTEGER,
    chamber TEXT,
    purpose TEXT,
    description TEXT,
    proposed_at TIMESTAMP WITH TIME ZONE,
    submitted_at TIMESTAMP WITH TIME ZONE,
    is_bill_amendment BOOLEAN,
    is_treaty_amendment BOOLEAN,
    is_amendment_amendment BOOLEAN,
    notes TEXT,
    actions_count INTEGER,
    cosponsors_count INTEGER,
    amendments_to_amendment_count INTEGER,
    updated_at TIMESTAMP WITH TIME ZONE,
    FOREIGN KEY (congress) REFERENCES bicam_congressional.congresses (congress_number)
        DEFERRABLE INITIALLY DEFERRED
);

-- Dependent on bills and version codes:
CREATE TABLE IF NOT EXISTS bicam_congressional.bills_summaries(
    bill_id TEXT,
    action_date DATE,
    action_desc TEXT,
    text TEXT,
    version_code INTEGER,
    FOREIGN KEY (bill_id) REFERENCES bicam_congressional.bills (bill_id)
        DEFERRABLE INITIALLY DEFERRED,
    UNIQUE (bill_id, action_date, action_desc, version_code)
);

-- Dependent on bills and title type codes:
CREATE TABLE IF NOT EXISTS bicam_congressional.bills_titles(
    bill_id TEXT,
    title TEXT,
    title_type TEXT,
    bill_text_version_code TEXT,
    bill_text_version_name TEXT,
    chamber TEXT,
    title_type_code INTEGER,
    FOREIGN KEY (bill_id) REFERENCES bicam_congressional.bills (bill_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (title_type_code) REFERENCES bicam_congressional.ref_title_type_codes (title_type_code)
        DEFERRABLE INITIALLY DEFERRED,
    PRIMARY KEY (bill_id, title, title_type)
);

-- Depends only on congresses
CREATE TABLE IF NOT EXISTS bicam_congressional.committeeprints(
    print_id TEXT PRIMARY KEY,
    print_jacketnumber TEXT,
    congress INTEGER,
    chamber TEXT,
    title TEXT,
    print_number TEXT,
    citation TEXT,
    updated_at TIMESTAMP,
    FOREIGN KEY (congress) REFERENCES bicam_congressional.congresses (congress_number)
        DEFERRABLE INITIALLY DEFERRED
);

-- No direct FKs, but will link later
CREATE TABLE IF NOT EXISTS bicam_congressional.committeereports(
    report_id TEXT PRIMARY KEY,
    citation TEXT,
    report_type TEXT,
    report_number INTEGER,
    report_part INTEGER,
    congress INTEGER,
    session INTEGER,
    title TEXT,
    chamber TEXT,
    is_conference_report BOOLEAN,
    issued_at TIMESTAMP WITH TIME ZONE,
    texts_count INTEGER,
    updated_at TIMESTAMP WITH TIME ZONE
);

-- Depends on congresses and must link to bills, treaties, nominations, hearings
CREATE TABLE IF NOT EXISTS bicam_congressional.committeemeetings(
    meeting_id TEXT PRIMARY KEY,
    title TEXT,
    meeting_type TEXT,
    chamber TEXT,
    congress INTEGER,
    date TIMESTAMP,
    room TEXT,
    street_address TEXT,
    building TEXT,
    city TEXT,
    state TEXT,
    zip_code TEXT,
    meeting_status TEXT,
    updated_at TIMESTAMP WITH TIME ZONE,
    FOREIGN KEY (congress) REFERENCES bicam_congressional.congresses (congress_number)
        DEFERRABLE INITIALLY DEFERRED
);

-- Now create the amendments dependent tables
CREATE TABLE IF NOT EXISTS bicam_congressional.amendments_sponsors(
    amendment_id TEXT,
    bioguide_id TEXT,
    FOREIGN KEY (amendment_id) REFERENCES bicam_congressional.amendments (amendment_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (bioguide_id) REFERENCES bicam_congressional.members (bioguide_id)
        DEFERRABLE INITIALLY DEFERRED,
    PRIMARY KEY (amendment_id, bioguide_id)
);

CREATE TABLE IF NOT EXISTS bicam_congressional.amendments_cosponsors(
    amendment_id TEXT,
    bioguide_id TEXT,
    FOREIGN KEY (amendment_id) REFERENCES bicam_congressional.amendments (amendment_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (bioguide_id) REFERENCES bicam_congressional.members (bioguide_id)
        DEFERRABLE INITIALLY DEFERRED,
    PRIMARY KEY (amendment_id, bioguide_id)
);

CREATE TABLE IF NOT EXISTS bicam_congressional.amendments_amended_bills(
    amendment_id TEXT,
    bill_id TEXT,
    FOREIGN KEY (amendment_id) REFERENCES bicam_congressional.amendments (amendment_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (bill_id) REFERENCES bicam_congressional.bills (bill_id)
        DEFERRABLE INITIALLY DEFERRED,
    PRIMARY KEY (amendment_id, bill_id)
);

CREATE TABLE IF NOT EXISTS bicam_congressional.amendments_amended_treaties(
    amendment_id TEXT,
    treaty_id TEXT,
    FOREIGN KEY (amendment_id) REFERENCES bicam_congressional.amendments (amendment_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (treaty_id) REFERENCES bicam_congressional.treaties (treaty_id)
        DEFERRABLE INITIALLY DEFERRED,
    PRIMARY KEY (amendment_id, treaty_id)
);

CREATE TABLE IF NOT EXISTS bicam_congressional.amendments_amended_amendments(
    amendment_id TEXT,
    amended_amendment_id TEXT,
    FOREIGN KEY (amendment_id) REFERENCES bicam_congressional.amendments (amendment_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (amended_amendment_id) REFERENCES bicam_congressional.amendments (amendment_id)
        DEFERRABLE INITIALLY DEFERRED,
    PRIMARY KEY (amendment_id, amended_amendment_id)
);

CREATE TABLE IF NOT EXISTS bicam_congressional.amendments_actions(
    action_id TEXT PRIMARY KEY,
    amendment_id TEXT,
    action_code TEXT,
    action_date TIMESTAMP WITH TIME ZONE,
    text TEXT,
    action_type TEXT,
    source_system TEXT,
    source_system_code INTEGER,
    FOREIGN KEY (amendment_id) REFERENCES bicam_congressional.amendments (amendment_id)
        DEFERRABLE INITIALLY DEFERRED,
    UNIQUE (action_id, amendment_id)
);

CREATE TABLE IF NOT EXISTS bicam_congressional.amendments_actions_recorded_votes(
    action_id TEXT,
    amendment_id TEXT,
    chamber TEXT,
    congress INTEGER,
    date TIMESTAMP WITH TIME ZONE,
    roll_number INTEGER,
    session INTEGER,
    url TEXT,
    FOREIGN KEY (action_id, amendment_id) REFERENCES bicam_congressional.amendments_actions (action_id, amendment_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (congress) REFERENCES bicam_congressional.congresses (congress_number)
        DEFERRABLE INITIALLY DEFERRED,
    UNIQUE (action_id, amendment_id, url)
);

CREATE TABLE IF NOT EXISTS bicam_congressional.amendments_texts(
    amendment_id TEXT,
    date TIMESTAMP WITH TIME ZONE,
    type TEXT,
    raw_text TEXT,
    pdf TEXT,
    html TEXT,
    FOREIGN KEY (amendment_id) REFERENCES bicam_congressional.amendments (amendment_id)
        DEFERRABLE INITIALLY DEFERRED,
    PRIMARY KEY (amendment_id, date, type)
);

-- Bills related tables
CREATE TABLE IF NOT EXISTS bicam_congressional.bills_texts(
    bill_id TEXT,
    date TEXT,
    type TEXT,
    raw_text TEXT,
    formatted_text TEXT,
    pdf TEXT,
    xml TEXT,
    FOREIGN KEY (bill_id) REFERENCES bicam_congressional.bills (bill_id)
        DEFERRABLE INITIALLY DEFERRED,
    UNIQUE (bill_id, date, type)
);

CREATE TABLE IF NOT EXISTS bicam_congressional.bills_actions(
    action_id TEXT PRIMARY KEY,
    bill_id TEXT,
    action_code TEXT,
    action_date DATE,
    text TEXT,
    action_type TEXT,
    source_system TEXT,
    source_system_code INTEGER,
    calendar TEXT,
    calendar_number INTEGER,
    FOREIGN KEY (bill_id) REFERENCES bicam_congressional.bills (bill_id)
        DEFERRABLE INITIALLY DEFERRED,
    UNIQUE (action_id, bill_id)
);

CREATE TABLE IF NOT EXISTS bicam_congressional.bills_actions_committees(
    action_id TEXT,
    bill_id TEXT,
    committee_code TEXT,
    FOREIGN KEY (action_id, bill_id) REFERENCES bicam_congressional.bills_actions (action_id, bill_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (committee_code) REFERENCES bicam_congressional.committees (committee_code)
        DEFERRABLE INITIALLY DEFERRED,
    UNIQUE (action_id, bill_id, committee_code)
);

CREATE TABLE IF NOT EXISTS bicam_congressional.bills_actions_recorded_votes(
    action_id TEXT,
    bill_id TEXT,
    chamber TEXT,
    congress INTEGER,
    date TIMESTAMP WITH TIME ZONE,
    roll_number INTEGER,
    session INTEGER,
    url TEXT,
    FOREIGN KEY (action_id, bill_id) REFERENCES bicam_congressional.bills_actions (action_id, bill_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (congress) REFERENCES bicam_congressional.congresses (congress_number)
        DEFERRABLE INITIALLY DEFERRED,
    UNIQUE (action_id, bill_id, url)
);

CREATE TABLE IF NOT EXISTS bicam_congressional.bills_related_bills(
    bill_id TEXT,
    related_bill_id TEXT,
    identification_entity TEXT,
    FOREIGN KEY (bill_id) REFERENCES bicam_congressional.bills (bill_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (related_bill_id) REFERENCES bicam_congressional.bills (bill_id)
        DEFERRABLE INITIALLY DEFERRED,
    PRIMARY KEY (bill_id, related_bill_id)
);

CREATE TABLE IF NOT EXISTS bicam_congressional.bills_cbocostestimates(
    bill_id TEXT,
    description TEXT,
    pub_date TIMESTAMP WITH TIME ZONE,
    title TEXT,
    url TEXT,
    FOREIGN KEY (bill_id) REFERENCES bicam_congressional.bills (bill_id)
        DEFERRABLE INITIALLY DEFERRED,
    PRIMARY KEY (bill_id, pub_date, url)
);

CREATE TABLE IF NOT EXISTS bicam_congressional.bills_cosponsors(
    bill_id TEXT,
    bioguide_id TEXT,
    FOREIGN KEY (bill_id) REFERENCES bicam_congressional.bills (bill_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (bioguide_id) REFERENCES bicam_congressional.members (bioguide_id)
        DEFERRABLE INITIALLY DEFERRED,
    PRIMARY KEY (bill_id, bioguide_id)
);

CREATE TABLE IF NOT EXISTS bicam_congressional.bills_laws(
    bill_id TEXT,
    law_id TEXT,
    law_number TEXT,
    law_type TEXT,
    FOREIGN KEY (bill_id) REFERENCES bicam_congressional.bills (bill_id)
        DEFERRABLE INITIALLY DEFERRED,
    PRIMARY KEY (bill_id, law_id)
);

CREATE TABLE IF NOT EXISTS bicam_congressional.bills_notes(
    bill_id TEXT,
    note_number INTEGER,
    note_text TEXT,
    updated_at TIMESTAMP WITH TIME ZONE,
    FOREIGN KEY (bill_id) REFERENCES bicam_congressional.bills (bill_id)
        DEFERRABLE INITIALLY DEFERRED,
    PRIMARY KEY (bill_id, note_number)
);

CREATE TABLE IF NOT EXISTS bicam_congressional.bills_notes_links(
    bill_id TEXT,
    note_number INTEGER,
    link_name TEXT,
    link_url TEXT,
    updated_at TIMESTAMP WITH TIME ZONE,
    FOREIGN KEY (bill_id, note_number) REFERENCES bicam_congressional.bills_notes (bill_id, note_number)
        DEFERRABLE INITIALLY DEFERRED,
    UNIQUE (bill_id, note_number, link_url)
);

CREATE TABLE IF NOT EXISTS bicam_congressional.bills_sponsors(
    bill_id TEXT,
    bioguide_id TEXT,
    FOREIGN KEY (bill_id) REFERENCES bicam_congressional.bills (bill_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (bioguide_id) REFERENCES bicam_congressional.members (bioguide_id)
        DEFERRABLE INITIALLY DEFERRED,
    PRIMARY KEY (bill_id, bioguide_id)
);

CREATE TABLE IF NOT EXISTS bicam_congressional.bills_subjects(
    bill_id TEXT,
    subject TEXT,
    updated_at TIMESTAMP WITH TIME ZONE,
    FOREIGN KEY (bill_id) REFERENCES bicam_congressional.bills (bill_id)
        DEFERRABLE INITIALLY DEFERRED,
    PRIMARY KEY (bill_id, subject)
);

-- committeeprints related
CREATE TABLE IF NOT EXISTS bicam_congressional.committeeprints_associated_bills(
    print_id TEXT,
    bill_id TEXT,
    FOREIGN KEY (print_id) REFERENCES bicam_congressional.committeeprints (print_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (bill_id) REFERENCES bicam_congressional.bills (bill_id)
        DEFERRABLE INITIALLY DEFERRED,
    PRIMARY KEY (print_id, bill_id)
);

CREATE TABLE IF NOT EXISTS bicam_congressional.committeeprints_committees(
    print_id TEXT,
    committee_code TEXT,
    FOREIGN KEY (print_id) REFERENCES bicam_congressional.committeeprints (print_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (committee_code) REFERENCES bicam_congressional.committees (committee_code)
        DEFERRABLE INITIALLY DEFERRED,
    PRIMARY KEY (print_id, committee_code)
);

CREATE TABLE IF NOT EXISTS bicam_congressional.committeeprints_texts(
    print_id TEXT,
    raw_text TEXT,
    formatted_text TEXT,
    pdf TEXT,
    html TEXT,
    xml TEXT,
    png TEXT,
    FOREIGN KEY (print_id) REFERENCES bicam_congressional.committeeprints (print_id)
        DEFERRABLE INITIALLY DEFERRED,
    UNIQUE (print_id, formatted_text, pdf, html, xml)
);

-- committeereports related
CREATE TABLE IF NOT EXISTS bicam_congressional.committeereports_associated_bills(
    report_id TEXT,
    bill_id TEXT,
    FOREIGN KEY (report_id) REFERENCES bicam_congressional.committeereports (report_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (bill_id) REFERENCES bicam_congressional.bills (bill_id)
        DEFERRABLE INITIALLY DEFERRED,
    PRIMARY KEY (report_id, bill_id)
);

CREATE TABLE IF NOT EXISTS bicam_congressional.committeereports_associated_treaties(
    report_id TEXT,
    treaty_id TEXT,
    FOREIGN KEY (report_id) REFERENCES bicam_congressional.committeereports (report_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (treaty_id) REFERENCES bicam_congressional.treaties (treaty_id)
        DEFERRABLE INITIALLY DEFERRED,
    PRIMARY KEY (report_id, treaty_id)
);

CREATE TABLE IF NOT EXISTS bicam_congressional.committeereports_texts(
    report_id TEXT,
    raw_text TEXT,
    formatted_text TEXT,
    formatted_text_is_errata BOOLEAN,
    pdf TEXT,
    pdf_is_errata BOOLEAN,
    FOREIGN KEY (report_id) REFERENCES bicam_congressional.committeereports (report_id)
        DEFERRABLE INITIALLY DEFERRED,
    UNIQUE (report_id, formatted_text, pdf)
);

-- committees related
CREATE TABLE IF NOT EXISTS bicam_congressional.committees_bills(
    committee_code TEXT,
    bill_id TEXT,
    relationship_type TEXT,
    committee_action_date TIMESTAMP WITH TIME ZONE,
    updated_at TIMESTAMP WITH TIME ZONE,
    FOREIGN KEY (committee_code) REFERENCES bicam_congressional.committees (committee_code)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (bill_id) REFERENCES bicam_congressional.bills (bill_id)
        DEFERRABLE INITIALLY DEFERRED,
    UNIQUE (committee_code, bill_id, relationship_type, committee_action_date)
);

CREATE TABLE IF NOT EXISTS bicam_congressional.committees_history(
    committee_code TEXT,
    name TEXT,
    loc_name TEXT,
    started_at TIMESTAMP WITH TIME ZONE,
    ended_at TIMESTAMP WITH TIME ZONE,
    committee_type TEXT,
    establishing_authority TEXT,
    su_doc_class_number TEXT,
    nara_id TEXT,
    loc_linked_data_id TEXT,
    updated_at TIMESTAMP WITH TIME ZONE,
    FOREIGN KEY (committee_code) REFERENCES bicam_congressional.committees (committee_code)
        DEFERRABLE INITIALLY DEFERRED,
    UNIQUE (committee_code, started_at, ended_at)
);

CREATE TABLE IF NOT EXISTS bicam_congressional.committees_subcommittees(
    committee_code TEXT,
    subcommittee_code TEXT,
    FOREIGN KEY (committee_code) REFERENCES bicam_congressional.committees (committee_code)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (subcommittee_code) REFERENCES bicam_congressional.committees (committee_code)
        DEFERRABLE INITIALLY DEFERRED,
    PRIMARY KEY (committee_code, subcommittee_code)
);

CREATE TABLE IF NOT EXISTS bicam_congressional.committees_committeereports(
    committee_code TEXT,
    report_id TEXT,
    FOREIGN KEY (committee_code) REFERENCES bicam_congressional.committees (committee_code)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (report_id) REFERENCES bicam_congressional.committeereports (report_id)
        DEFERRABLE INITIALLY DEFERRED,
    PRIMARY KEY (committee_code, report_id)
);

-- hearings related
CREATE TABLE IF NOT EXISTS bicam_congressional.hearings_committees(
    hearing_id TEXT,
    committee_code TEXT,
    FOREIGN KEY (hearing_id) REFERENCES bicam_congressional.hearings (hearing_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (committee_code) REFERENCES bicam_congressional.committees (committee_code)
        DEFERRABLE INITIALLY DEFERRED,
    PRIMARY KEY (hearing_id, committee_code)
);

CREATE TABLE IF NOT EXISTS bicam_congressional.hearings_dates(
    hearing_id TEXT,
    hearing_date DATE,
    FOREIGN KEY (hearing_id) REFERENCES bicam_congressional.hearings (hearing_id)
        DEFERRABLE INITIALLY DEFERRED,
    PRIMARY KEY (hearing_id, hearing_date)
);

CREATE TABLE IF NOT EXISTS bicam_congressional.hearings_texts(
    hearing_id TEXT,
    raw_text TEXT,
    pdf TEXT,
    formatted_text TEXT,
    FOREIGN KEY (hearing_id) REFERENCES bicam_congressional.hearings (hearing_id)
        DEFERRABLE INITIALLY DEFERRED,
    UNIQUE (hearing_id, pdf, formatted_text)
);

-- members related
CREATE TABLE IF NOT EXISTS bicam_congressional.members_terms(
    bioguide_id TEXT,
    member_type TEXT,
    chamber TEXT,
    congress INTEGER,
    start_year INTEGER,
    end_year INTEGER,
    state_name TEXT,
    state_code TEXT,
    district INTEGER,
    FOREIGN KEY (bioguide_id) REFERENCES bicam_congressional.members (bioguide_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (congress) REFERENCES bicam_congressional.congresses (congress_number)
        DEFERRABLE INITIALLY DEFERRED,
    PRIMARY KEY (bioguide_id, start_year, end_year)
);

CREATE TABLE IF NOT EXISTS bicam_congressional.members_leadership_roles(
    bioguide_id TEXT,
    role TEXT,
    congress INTEGER,
    chamber TEXT,
    is_current BOOLEAN,
    FOREIGN KEY (bioguide_id) REFERENCES bicam_congressional.members (bioguide_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (congress) REFERENCES bicam_congressional.congresses (congress_number)
        DEFERRABLE INITIALLY DEFERRED,
    UNIQUE (bioguide_id, role, congress, chamber)
);

CREATE TABLE IF NOT EXISTS bicam_congressional.members_party_history(
    bioguide_id TEXT,
    party_code TEXT,
    party_name TEXT,
    start_year INTEGER,
    end_year INTEGER,
    FOREIGN KEY (bioguide_id) REFERENCES bicam_congressional.members (bioguide_id)
        DEFERRABLE INITIALLY DEFERRED,
    UNIQUE (bioguide_id, party_code, start_year, end_year)
);

-- nominations related
CREATE TABLE IF NOT EXISTS bicam_congressional.nominations_actions(
    action_id TEXT PRIMARY KEY,
    nomination_id TEXT,
    action_code TEXT,
    action_type TEXT,
    action_date DATE,
    text TEXT,
    FOREIGN KEY (nomination_id) REFERENCES bicam_congressional.nominations (nomination_id)
        DEFERRABLE INITIALLY DEFERRED,
    UNIQUE (action_id, nomination_id)
);

CREATE TABLE IF NOT EXISTS bicam_congressional.nominations_positions(
    nomination_id TEXT,
    ordinal INTEGER,
    position_title TEXT,
    organization TEXT,
    intro_text TEXT,
    nominee_count INTEGER,
    FOREIGN KEY (nomination_id) REFERENCES bicam_congressional.nominations (nomination_id)
        DEFERRABLE INITIALLY DEFERRED,
    PRIMARY KEY (nomination_id, ordinal)
);

CREATE TABLE IF NOT EXISTS bicam_congressional.nominations_actions_committees(
    action_id TEXT,
    nomination_id TEXT,
    committee_code TEXT,
    FOREIGN KEY (action_id, nomination_id) REFERENCES bicam_congressional.nominations_actions (action_id, nomination_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (committee_code) REFERENCES bicam_congressional.committees (committee_code)
        DEFERRABLE INITIALLY DEFERRED,
    PRIMARY KEY (action_id, nomination_id, committee_code)
);

CREATE TABLE IF NOT EXISTS bicam_congressional.nominations_committeeactivities(
    nomination_id TEXT,
    committee_code TEXT,
    activity_name TEXT,
    activity_date TIMESTAMP WITH TIME ZONE,
    FOREIGN KEY (nomination_id) REFERENCES bicam_congressional.nominations (nomination_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (committee_code) REFERENCES bicam_congressional.committees (committee_code)
        DEFERRABLE INITIALLY DEFERRED,
    UNIQUE (nomination_id, committee_code, activity_date, activity_name)
);

CREATE TABLE IF NOT EXISTS bicam_congressional.nominations_associated_hearings(
    nomination_id TEXT,
    hearing_id TEXT,
    FOREIGN KEY (nomination_id) REFERENCES bicam_congressional.nominations (nomination_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (hearing_id) REFERENCES bicam_congressional.hearings (hearing_id)
        DEFERRABLE INITIALLY DEFERRED,
    PRIMARY KEY (nomination_id, hearing_id)
);

CREATE TABLE IF NOT EXISTS bicam_congressional.nominations_nominees(
    nomination_id TEXT,
    ordinal INTEGER,
    first_name TEXT,
    middle_name TEXT,
    last_name TEXT,
    prefix TEXT,
    suffix TEXT,
    state TEXT,
    effective_date DATE,
    predecessor_name TEXT,
    corps_code TEXT,
    FOREIGN KEY (nomination_id, ordinal) REFERENCES bicam_congressional.nominations_positions (nomination_id, ordinal)
        DEFERRABLE INITIALLY DEFERRED,
    UNIQUE (nomination_id, ordinal, first_name, middle_name, last_name)
);

-- treaties related
CREATE TABLE IF NOT EXISTS bicam_congressional.treaties_actions(
    action_id TEXT PRIMARY KEY,
    treaty_id TEXT,
    action_code TEXT,
    action_date DATE,
    text TEXT,
    action_type TEXT,
    FOREIGN KEY (treaty_id) REFERENCES bicam_congressional.treaties (treaty_id)
        DEFERRABLE INITIALLY DEFERRED,
    UNIQUE (action_id, treaty_id)
);

CREATE TABLE IF NOT EXISTS bicam_congressional.treaties_actions_committees(
    action_id TEXT,
    treaty_id TEXT,
    committee_code TEXT,
    FOREIGN KEY (action_id, treaty_id) REFERENCES bicam_congressional.treaties_actions (action_id, treaty_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (committee_code) REFERENCES bicam_congressional.committees (committee_code)
        DEFERRABLE INITIALLY DEFERRED,
    PRIMARY KEY (action_id, treaty_id, committee_code)
);

CREATE TABLE IF NOT EXISTS bicam_congressional.treaties_country_parties(
    treaty_id TEXT,
    country TEXT,
    FOREIGN KEY (treaty_id) REFERENCES bicam_congressional.treaties (treaty_id)
        DEFERRABLE INITIALLY DEFERRED,
    PRIMARY KEY (treaty_id, country)
);

CREATE TABLE IF NOT EXISTS bicam_congressional.treaties_index_terms(
    treaty_id TEXT,
    index_term TEXT,
    FOREIGN KEY (treaty_id) REFERENCES bicam_congressional.treaties (treaty_id)
        DEFERRABLE INITIALLY DEFERRED,
    PRIMARY KEY (treaty_id, index_term)
);

CREATE TABLE IF NOT EXISTS bicam_congressional.treaties_titles(
    treaty_id TEXT,
    title TEXT,
    title_type TEXT,
    FOREIGN KEY (treaty_id) REFERENCES bicam_congressional.treaties (treaty_id)
        DEFERRABLE INITIALLY DEFERRED,
    UNIQUE (treaty_id, title, title_type)
);

-- committeemeetings related (depends on bills, treaties, nominations, hearings)
CREATE TABLE IF NOT EXISTS bicam_congressional.committeemeetings_associated_bills(
    meeting_id TEXT,
    bill_id TEXT,
    FOREIGN KEY (meeting_id) REFERENCES bicam_congressional.committeemeetings (meeting_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (bill_id) REFERENCES bicam_congressional.bills (bill_id)
        DEFERRABLE INITIALLY DEFERRED,
    PRIMARY KEY (meeting_id, bill_id)
);

CREATE TABLE IF NOT EXISTS bicam_congressional.committeemeetings_associated_treaties(
    meeting_id TEXT,
    treaty_id TEXT,
    FOREIGN KEY (meeting_id) REFERENCES bicam_congressional.committeemeetings (meeting_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (treaty_id) REFERENCES bicam_congressional.treaties (treaty_id)
        DEFERRABLE INITIALLY DEFERRED,
    PRIMARY KEY (meeting_id, treaty_id)
);

CREATE TABLE IF NOT EXISTS bicam_congressional.committeemeetings_associated_nominations(
    meeting_id TEXT,
    nomination_id TEXT,
    FOREIGN KEY (meeting_id) REFERENCES bicam_congressional.committeemeetings (meeting_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (nomination_id) REFERENCES bicam_congressional.nominations (nomination_id)
        DEFERRABLE INITIALLY DEFERRED,
    PRIMARY KEY (meeting_id, nomination_id)
);

CREATE TABLE IF NOT EXISTS bicam_congressional.committeemeetings_meeting_documents(
    meeting_id TEXT,
    name TEXT,
    document_type TEXT,
    description TEXT,
    url TEXT,
    FOREIGN KEY (meeting_id) REFERENCES bicam_congressional.committeemeetings (meeting_id)
        DEFERRABLE INITIALLY DEFERRED,
    UNIQUE (meeting_id, url)
);

CREATE TABLE IF NOT EXISTS bicam_congressional.committeemeetings_witness_documents(
    meeting_id TEXT,
    document_type TEXT,
    url TEXT,
    FOREIGN KEY (meeting_id) REFERENCES bicam_congressional.committeemeetings (meeting_id)
        DEFERRABLE INITIALLY DEFERRED,
    UNIQUE (meeting_id, url)
);

CREATE TABLE IF NOT EXISTS bicam_congressional.committeemeetings_witnesses(
    meeting_id TEXT,
    name TEXT,
    position TEXT,
    organization TEXT,
    FOREIGN KEY (meeting_id) REFERENCES bicam_congressional.committeemeetings (meeting_id)
        DEFERRABLE INITIALLY DEFERRED,
    UNIQUE (meeting_id, name, position, organization)
);

CREATE TABLE IF NOT EXISTS bicam_congressional.committeemeetings_associated_hearings(
    meeting_id TEXT,
    hearing_id TEXT,
    FOREIGN KEY (meeting_id) REFERENCES bicam_congressional.committeemeetings (meeting_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (hearing_id) REFERENCES bicam_congressional.hearings (hearing_id)
        DEFERRABLE INITIALLY DEFERRED,
    PRIMARY KEY (meeting_id, hearing_id)
);

CREATE TABLE IF NOT EXISTS bicam_congressional.committeemeetings_committees(
    meeting_id TEXT,
    committee_code TEXT,
    FOREIGN KEY (meeting_id) REFERENCES bicam_congressional.committeemeetings (meeting_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (committee_code) REFERENCES bicam_congressional.committees (committee_code)
        DEFERRABLE INITIALLY DEFERRED,
    PRIMARY KEY (meeting_id, committee_code)
);

-- Finish off with granting permissions
DO $$
DECLARE
    current_table_name text;
    current_schema_name text;
BEGIN
    FOR current_schema_name IN SELECT schema_name FROM information_schema.schemata WHERE schema_name IN ('bicam_staging_congressional', 'bicam_congressional') LOOP
        FOR current_table_name IN SELECT table_name FROM information_schema.tables WHERE table_schema = current_schema_name LOOP
            EXECUTE format('GRANT ALL ON TABLE %I.%I TO bicam_pipeline', current_schema_name, current_table_name);
        END LOOP;
    END LOOP;
END$$;

END TRANSACTION;

SET CONSTRAINTS ALL IMMEDIATE;
COMMIT;

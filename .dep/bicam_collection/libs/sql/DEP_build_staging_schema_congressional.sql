-- Create schema for staging congressional data
BEGIN;

-- Bills Table
CREATE TABLE IF NOT EXISTS bicam_staging_congressional.bills_staging(
    bill_id TEXT,
    bill_type TEXT,
    bill_number TEXT,
    congress INTEGER,
    title TEXT,
    origin_chamber TEXT,
    origin_chamber_code TEXT,
    introduced_at TIMESTAMP WITH TIME ZONE,
    constitutional_authority_statement TEXT,
    is_law BOOLEAN,
    law_number TEXT,
    law_type TEXT,
    notes TEXT,
    policy_area TEXT,
    actions_count INTEGER,
    amendments_count INTEGER,
    committees_count INTEGER,
    cosponsors_count INTEGER,
    related_bills_count INTEGER,
    summaries_count INTEGER,
    subjects_count INTEGER,
    titles_count INTEGER,
    texts_count INTEGER,
    updated_at TIMESTAMP WITH TIME ZONE,
    _inserted_at TIMESTAMP WITH TIME ZONE,
    UNIQUE (bill_id, bill_type, bill_number, congress, title, origin_chamber, origin_chamber_code, introduced_at, constitutional_authority_statement, is_law, law_number, law_type, notes, policy_area, actions_count, amendments_count, committees_count, cosponsors_count, related_bills_count, summaries_count, subjects_count, titles_count, texts_count, updated_at)
);

CREATE TABLE IF NOT EXISTS bicam_staging_congressional.bills_cbocostestimates_staging(
    bill_id TEXT,
    description TEXT,
    pub_date TIMESTAMP WITH TIME ZONE,
    title TEXT,
    url TEXT,
    _inserted_at TIMESTAMP WITH TIME ZONE,
    UNIQUE (bill_id, description, pub_date, title, url)
);

CREATE TABLE IF NOT EXISTS bicam_staging_congressional.bills_sponsors_staging(
    bill_id TEXT,
    sponsor_bioguide_id TEXT,
    _inserted_at TIMESTAMP WITH TIME ZONE,
    UNIQUE (bill_id, sponsor_bioguide_id)
);

CREATE TABLE IF NOT EXISTS bicam_staging_congressional.bills_committeereports_staging(
    bill_id TEXT,
    report_id TEXT,
    _inserted_at TIMESTAMP WITH TIME ZONE,
    UNIQUE (bill_id, report_id)
);

CREATE TABLE IF NOT EXISTS bicam_staging_congressional.bills_actions_staging(
    bill_id TEXT,
    action_id TEXT,
    action_code TEXT,
    action_date TIMESTAMP WITH TIME ZONE,
    text TEXT,
    action_type TEXT,
    source_system TEXT,
    source_system_code TEXT,
    calendar TEXT,
    calendar_number TEXT,
    _inserted_at TIMESTAMP WITH TIME ZONE,
    UNIQUE (bill_id, action_id, action_code, action_date, text, action_type, source_system, source_system_code, calendar, calendar_number)
);

CREATE TABLE IF NOT EXISTS bicam_staging_congressional.bills_actions_committee_codes_staging(
    bill_id TEXT,
    action_code TEXT,
    action_date TIMESTAMP WITH TIME ZONE,
    text TEXT,
    committee_codes TEXT,
    _inserted_at TIMESTAMP WITH TIME ZONE,
    UNIQUE (bill_id, action_code, action_date, text, committee_codes)
);

CREATE TABLE IF NOT EXISTS bicam_staging_congressional.bills_actions_recorded_votes_staging(
    bill_id TEXT,
    action_code TEXT,
    action_date TIMESTAMP WITH TIME ZONE,
    text TEXT,
    chamber TEXT,
    congress TEXT,
    date TIMESTAMP WITH TIME ZONE,
    roll_number INTEGER,
    session_number INTEGER,
    url TEXT,
    _inserted_at TIMESTAMP WITH TIME ZONE,
    UNIQUE (bill_id, action_code, action_date, text, chamber, congress, date, roll_number, session_number, url)
);

CREATE TABLE IF NOT EXISTS bicam_staging_congressional.bills_committeeactivities_staging(
    bill_id TEXT,
    committee_code TEXT,
    committee_name TEXT,
    activity_name TEXT,
    activity_date TIMESTAMP WITH TIME ZONE,
    subcommittee_code TEXT,
    subcommittee_name TEXT,
    subcommittee_activity_name TEXT,
    subcommittee_activity_date TIMESTAMP WITH TIME ZONE,
    _inserted_at TIMESTAMP WITH TIME ZONE,
    UNIQUE (bill_id, committee_code, committee_name, activity_name, activity_date, subcommittee_code, subcommittee_name, subcommittee_activity_name, subcommittee_activity_date)
);

CREATE TABLE IF NOT EXISTS bicam_staging_congressional.bills_cosponsors_staging(
    bill_id TEXT,
    bioguide_id TEXT,
    _inserted_at TIMESTAMP WITH TIME ZONE,
    UNIQUE (bill_id, bioguide_id)
);

CREATE TABLE IF NOT EXISTS bicam_staging_congressional.bills_billrelations_staging(
    bill_id TEXT,
    relatedbill_id TEXT,
    relationship_identified_by_1 TEXT,
    relationship_type_1 TEXT,
    relationship_identified_by_2 TEXT,
    relationship_type_2 TEXT,
    relationship_identified_by_3 TEXT,
    relationship_type_3 TEXT,
    _inserted_at TIMESTAMP WITH TIME ZONE,
    UNIQUE (bill_id, relatedbill_id, relationship_identified_by_1, relationship_type_1, relationship_identified_by_2, relationship_type_2, relationship_identified_by_3, relationship_type_3)
);

CREATE TABLE IF NOT EXISTS bicam_staging_congressional.bills_texts_staging(
    bill_id TEXT,
    date TEXT,
    type TEXT,
    formatted_text TEXT,
    pdf TEXT,
    html TEXT,
    xml TEXT,
    _inserted_at TIMESTAMP WITH TIME ZONE,
    UNIQUE (bill_id, date, type, formatted_text, pdf, html, xml)
);

CREATE TABLE IF NOT EXISTS bicam_staging_congressional.bills_titles_staging(
    bill_id TEXT,
    title TEXT,
    title_type TEXT,
    bill_text_version_code TEXT,
    bill_text_version_name TEXT,
    chamber_code TEXT,
    chamber_name TEXT,
    title_type_code TEXT,
    _inserted_at TIMESTAMP WITH TIME ZONE,
    UNIQUE (bill_id, title, title_type, bill_text_version_code, bill_text_version_name, chamber_code, chamber_name, title_type_code)
);

CREATE TABLE IF NOT EXISTS bicam_staging_congressional.bills_summaries_staging(
    bill_id TEXT,
    action_date TIMESTAMP WITH TIME ZONE,
    action_desc TEXT,
    text TEXT,
    version_code TEXT,
    updated_at TIMESTAMP WITH TIME ZONE,
    _inserted_at TIMESTAMP WITH TIME ZONE,
    UNIQUE (bill_id, action_date, action_desc, text, version_code, updated_at)
);

CREATE TABLE IF NOT EXISTS bicam_staging_congressional.bills_subjects_staging(
    bill_id TEXT,
    subject TEXT,
    _inserted_at TIMESTAMP WITH TIME ZONE,
    UNIQUE (bill_id, subject)
);

CREATE TABLE IF NOT EXISTS bicam_staging_congressional.amendments_staging(
    amendment_id TEXT,
    amendment_type TEXT,
    number TEXT,
    congress INTEGER,
    chamber TEXT,
    purpose TEXT,
    description TEXT,
    proposed_at TIMESTAMP WITH TIME ZONE,
    submitted_at TIMESTAMP WITH TIME ZONE,
    amended_bill_id TEXT,
    amended_amendment_id TEXT,
    amended_treaty_id TEXT,
    notes TEXT,
    amendments_to_amendment_count INTEGER,
    texts_count INTEGER,
    updated_at TIMESTAMP WITH TIME ZONE,
    _inserted_at TIMESTAMP WITH TIME ZONE,
    UNIQUE (amendment_id, amendment_type, number, congress, chamber, purpose, description, proposed_at, submitted_at, amended_bill_id, amended_amendment_id, amended_treaty_id, notes, amendments_to_amendment_count, texts_count, updated_at)
);

CREATE TABLE IF NOT EXISTS bicam_staging_congressional.amendments_sponsor_bioguide_ids_staging(
    amendment_id TEXT,
    sponsor_bioguide_id TEXT,
    _inserted_at TIMESTAMP WITH TIME ZONE,
    UNIQUE (amendment_id, sponsor_bioguide_id)
);

CREATE TABLE IF NOT EXISTS bicam_staging_congressional.amendments_actions_staging(
    amendment_id TEXT,
    action_id TEXT,
    action_code TEXT,
    action_date TIMESTAMP WITH TIME ZONE,
    text TEXT,
    action_type TEXT,
    source_system TEXT,
    source_system_code TEXT,
    _inserted_at TIMESTAMP WITH TIME ZONE,
    UNIQUE (amendment_id, action_id, action_code, action_date, text, action_type, source_system, source_system_code)
);

CREATE TABLE IF NOT EXISTS bicam_staging_congressional.amendments_actions_recorded_votes_staging(
    amendment_id TEXT,
    action_code TEXT,
    action_date TIMESTAMP WITH TIME ZONE,
    text TEXT,
    chamber TEXT,
    congress TEXT,
    date TIMESTAMP WITH TIME ZONE,
    roll_number INTEGER,
    session_number INTEGER,
    url TEXT,
    _inserted_at TIMESTAMP WITH TIME ZONE,
    UNIQUE (amendment_id, action_code, action_date, text, chamber, congress, date, roll_number, session_number, url)
);

CREATE TABLE IF NOT EXISTS bicam_staging_congressional.amendments_cosponsors_staging(
    amendment_id TEXT,
    bioguide_id TEXT,
    _inserted_at TIMESTAMP WITH TIME ZONE,
    UNIQUE (amendment_id, bioguide_id)
);

CREATE TABLE IF NOT EXISTS bicam_staging_congressional.amendments_texts_staging(
    amendment_id TEXT,
    date TEXT,
    type TEXT,
    formatted_text TEXT,
    pdf TEXT,
    html TEXT,
    xml TEXT,
    _inserted_at TIMESTAMP WITH TIME ZONE,
    UNIQUE (amendment_id, date, type, formatted_text, pdf, html, xml)
);

CREATE TABLE IF NOT EXISTS bicam_staging_congressional.members_staging(
    bioguide_id TEXT,
    direct_order_name TEXT,
    inverted_order_name TEXT,
    honorific_name TEXT,
    first_name TEXT,
    middle_name TEXT,
    last_name TEXT,
    suffix_name TEXT,
    nickname TEXT,
    party TEXT,
    state TEXT,
    district TEXT,
    birth_year INTEGER,
    death_year INTEGER,
    official_url TEXT,
    office_address TEXT,
    office_city TEXT,
    office_district TEXT,
    office_zip TEXT,
    office_phone_number TEXT,
    sponsored_legislation_count INTEGER,
    cosponsored_legislation_count INTEGER,
    depiction_image_url TEXT,
    depiction_attribution TEXT,
    is_current_member BOOLEAN,
    updated_at TIMESTAMP WITH TIME ZONE,
    _inserted_at TIMESTAMP WITH TIME ZONE,
    UNIQUE (bioguide_id, direct_order_name, inverted_order_name, honorific_name, first_name, middle_name, last_name, suffix_name, nickname, party, state, district, birth_year, death_year, official_url, office_address, office_city, office_district, office_zip, office_phone_number, sponsored_legislation_count, cosponsored_legislation_count, depiction_image_url, depiction_attribution, is_current_member, updated_at)
);

CREATE TABLE IF NOT EXISTS bicam_staging_congressional.members_terms_staging(
    bioguide_id TEXT,
    member_type TEXT,
    chamber TEXT,
    congress INTEGER,
    start_year INTEGER,
    end_year INTEGER,
    state_name TEXT,
    state_code TEXT,
    district TEXT,
    party_code TEXT,
    party_name TEXT,
    _inserted_at TIMESTAMP WITH TIME ZONE,
    UNIQUE (bioguide_id, member_type, chamber, congress, start_year, end_year, state_name, state_code, district, party_code, party_name)
);

CREATE TABLE IF NOT EXISTS bicam_staging_congressional.members_leadership_roles_staging(
    bioguide_id TEXT,
    type TEXT,
    congress INTEGER,
    is_current BOOLEAN,
    _inserted_at TIMESTAMP WITH TIME ZONE,
    UNIQUE (bioguide_id, type, congress, is_current)
);

CREATE TABLE IF NOT EXISTS bicam_staging_congressional.members_party_history_staging(
    bioguide_id TEXT NOT NULL,
    party_code TEXT,
    party_name TEXT,
    start_year INTEGER,
    end_year INTEGER,
    _inserted_at TIMESTAMP WITH TIME ZONE,
    UNIQUE (bioguide_id, party_code, start_year, end_year)
    );

CREATE TABLE IF NOT EXISTS bicam_staging_congressional.committees_staging(
    committee_code TEXT,
    name TEXT,
    type TEXT,
    chamber TEXT,
    is_subcommittee BOOLEAN,
    is_current BOOLEAN,
    bills_count INTEGER,
    reports_count INTEGER,
    nominations_count INTEGER,
    updated_at TIMESTAMP WITH TIME ZONE,
    _inserted_at TIMESTAMP WITH TIME ZONE,
    UNIQUE (committee_code, name, type, chamber, is_subcommittee, is_current, bills_count, reports_count, nominations_count, updated_at)
);

CREATE TABLE IF NOT EXISTS bicam_staging_congressional.committees_subcommittee_codes_staging(
    committee_code TEXT,
    subcommittee_codes TEXT,
    _inserted_at TIMESTAMP WITH TIME ZONE,
    UNIQUE (committee_code, subcommittee_codes)
);

CREATE TABLE IF NOT EXISTS bicam_staging_congressional.committees_history_staging(
    committee_code TEXT,
    official_name TEXT,
    loc_name TEXT,
    start_date TIMESTAMP WITH TIME ZONE,
    end_date TIMESTAMP WITH TIME ZONE,
    committee_type TEXT,
    establishing_authority TEXT,
    superintendent_document_number TEXT,
    nara_id TEXT,
    updated_at TIMESTAMP WITH TIME ZONE,
    _inserted_at TIMESTAMP WITH TIME ZONE,
    UNIQUE (committee_code, official_name, loc_name, start_date, end_date, committee_type, establishing_authority, superintendent_document_number, nara_id, updated_at)
);

CREATE TABLE IF NOT EXISTS bicam_staging_congressional.committees_bills_staging(
    committee_code TEXT,
    bill_id TEXT,
    relationship_type TEXT,
    committee_action_date TIMESTAMP WITH TIME ZONE,
    _inserted_at TIMESTAMP WITH TIME ZONE,
    UNIQUE (committee_code, bill_id, relationship_type, committee_action_date)
);

CREATE TABLE IF NOT EXISTS bicam_staging_congressional.committees_relatedreports_staging(
    committee_code TEXT,
    report_id TEXT,
    _inserted_at TIMESTAMP WITH TIME ZONE,
    UNIQUE (committee_code, report_id)
);

CREATE TABLE IF NOT EXISTS bicam_staging_congressional.committeereports_staging(
    report_id TEXT,
    citation TEXT,
    type TEXT,
    number TEXT,
    part TEXT,
    congress TEXT,
    chamber TEXT,
    title TEXT,
    issued_at TIMESTAMP WITH TIME ZONE,
    is_conference_report BOOLEAN,
    texts_count INTEGER,
    updated_at TIMESTAMP WITH TIME ZONE,
    _inserted_at TIMESTAMP WITH TIME ZONE,
    UNIQUE (report_id, citation, type, number, part, congress, chamber, title, issued_at, is_conference_report, texts_count, updated_at)
);

CREATE TABLE IF NOT EXISTS bicam_staging_congressional.committeereports_associated_bill_ids_staging(
    report_id TEXT,
    associated_bill_ids TEXT,
    _inserted_at TIMESTAMP WITH TIME ZONE,
    UNIQUE (report_id, associated_bill_ids)
);

CREATE TABLE IF NOT EXISTS bicam_staging_congressional.committeereports_associated_treaty_ids_staging(
    report_id TEXT,
    associated_treaty_ids TEXT,
    _inserted_at TIMESTAMP WITH TIME ZONE,
    UNIQUE (report_id, associated_treaty_ids)
);

CREATE TABLE IF NOT EXISTS bicam_staging_congressional.committeereports_texts_staging(
    report_id TEXT,
    date TEXT,
    type TEXT,
    formatted_text TEXT,
    formatted_text_is_errata BOOLEAN,
    pdf TEXT,
    pdf_is_errata BOOLEAN,
    html TEXT,
    html_is_errata BOOLEAN,
    xml TEXT,
    xml_is_errata BOOLEAN,
    url TEXT,
    _inserted_at TIMESTAMP WITH TIME ZONE,
    UNIQUE (report_id, date, type, formatted_text, formatted_text_is_errata, pdf, pdf_is_errata, html, html_is_errata, xml, xml_is_errata, url)
);

CREATE TABLE IF NOT EXISTS bicam_staging_congressional.committeeprints_staging(
    print_id TEXT,
    print_jacketnumber TEXT,
    title TEXT,
    chamber TEXT,
    congress INTEGER,
    number TEXT,
    citation TEXT,
    texts_count INTEGER,
    is_conference_report BOOLEAN,
    updated_at TIMESTAMP WITH TIME ZONE,
    _inserted_at TIMESTAMP WITH TIME ZONE,
    UNIQUE(print_id, print_jacketnumber, title, chamber, congress, number, citation, texts_count, is_conference_report, updated_at)
);

CREATE TABLE IF NOT EXISTS bicam_staging_congressional.committeeprints_associated_bill_ids_staging(
    print_id TEXT,
    associated_bill_ids TEXT,
    _inserted_at TIMESTAMP WITH TIME ZONE,
    UNIQUE (print_id, associated_bill_ids)
);

CREATE TABLE IF NOT EXISTS bicam_staging_congressional.committeeprints_committee_codes_staging(
    print_id TEXT,
    committee_codes TEXT,
    _inserted_at TIMESTAMP WITH TIME ZONE,
    UNIQUE (print_id, committee_codes)
);

CREATE TABLE IF NOT EXISTS bicam_staging_congressional.committeeprints_texts_staging(
    print_id TEXT,
    type TEXT,
    formatted_text TEXT,
    pdf TEXT,
    html TEXT,
    xml TEXT,
    _inserted_at TIMESTAMP WITH TIME ZONE,
    UNIQUE (print_id, type, formatted_text, pdf, html, xml)
);

CREATE TABLE IF NOT EXISTS bicam_staging_congressional.committeemeetings_staging(
    meeting_id TEXT,
    title TEXT,
    type TEXT,
    chamber TEXT,
    congress INTEGER,
    date TIMESTAMP WITH TIME ZONE,
    room TEXT,
    building TEXT,
    address TEXT,
    meeting_status TEXT,
    updated_at TIMESTAMP WITH TIME ZONE,
    _inserted_at TIMESTAMP WITH TIME ZONE,
    UNIQUE (meeting_id, title, type, chamber, congress, date, room, building, address, meeting_status, updated_at)
);

CREATE TABLE IF NOT EXISTS bicam_staging_congressional.committeemeetings_witnesses_staging(
    meeting_id TEXT,
    name TEXT,
    position TEXT,
    organization TEXT,
    _inserted_at TIMESTAMP WITH TIME ZONE,
    UNIQUE (meeting_id, name, position, organization)
);

CREATE TABLE IF NOT EXISTS bicam_staging_congressional.committeemeetings_witness_documents_staging(
    meeting_id TEXT,
    document_type TEXT,
    pdf TEXT,
    formatted_text TEXT,
    html TEXT,
    xml TEXT,
    url TEXT,
    _inserted_at TIMESTAMP WITH TIME ZONE,
    UNIQUE (meeting_id, document_type, pdf, formatted_text, html, xml, url)
);

CREATE TABLE IF NOT EXISTS bicam_staging_congressional.committeemeetings_meeting_documents_staging(
    meeting_id TEXT,
    name TEXT,
    document_type TEXT,
    description TEXT,
    pdf TEXT,
    formatted_text TEXT,
    html TEXT,
    xml TEXT,
    url TEXT,
    _inserted_at TIMESTAMP WITH TIME ZONE,
    UNIQUE (meeting_id, name, document_type, description, pdf, formatted_text, html, xml, url)
);

CREATE TABLE IF NOT EXISTS bicam_staging_congressional.committeemeetings_hearing_jacketnumbers_staging(
    meeting_id TEXT,
    hearing_jacketnumbers TEXT,
    _inserted_at TIMESTAMP WITH TIME ZONE,
    UNIQUE (meeting_id, hearing_jacketnumbers)
);

CREATE TABLE IF NOT EXISTS bicam_staging_congressional.committeemeetings_associated_bill_ids_staging(
    meeting_id TEXT,
    associated_bill_ids TEXT,
    _inserted_at TIMESTAMP WITH TIME ZONE,
    UNIQUE (meeting_id, associated_bill_ids)
);

CREATE TABLE IF NOT EXISTS bicam_staging_congressional.committeemeetings_associated_treaty_ids_staging(
    meeting_id TEXT,
    associated_treaty_ids TEXT,
    _inserted_at TIMESTAMP WITH TIME ZONE,
    UNIQUE (meeting_id, associated_treaty_ids)
);

CREATE TABLE IF NOT EXISTS bicam_staging_congressional.committeemeetings_associated_nomination_ids_staging(
    meeting_id TEXT,
    associated_nomination_ids TEXT,
    _inserted_at TIMESTAMP WITH TIME ZONE,
    UNIQUE (meeting_id, associated_nomination_ids)
);

CREATE TABLE IF NOT EXISTS bicam_staging_congressional.hearings_staging(
    jacket_number TEXT,
    loc_id TEXT,
    title TEXT,
    citation TEXT,
    congress INTEGER,
    chamber TEXT,
    number TEXT,
    part_number TEXT,
    updated_at TIMESTAMP WITH TIME ZONE,
    _inserted_at TIMESTAMP WITH TIME ZONE,
    UNIQUE (jacket_number, loc_id, title, citation, congress, chamber, number, part_number, updated_at)
);

CREATE TABLE IF NOT EXISTS bicam_staging_congressional.hearings_committee_codes_staging(
    jacket_number TEXT,
    committee_codes TEXT,
    _inserted_at TIMESTAMP WITH TIME ZONE,
    UNIQUE (jacket_number, committee_codes)
);

CREATE TABLE IF NOT EXISTS bicam_staging_congressional.hearings_dates_staging(
    jacket_number TEXT,
    dates TEXT,
    _inserted_at TIMESTAMP WITH TIME ZONE,
    UNIQUE (jacket_number, dates)
);

CREATE TABLE IF NOT EXISTS bicam_staging_congressional.hearings_formats_staging(
    jacket_number TEXT,
    pdf TEXT,
    formatted_text TEXT,
    _inserted_at TIMESTAMP WITH TIME ZONE,
    UNIQUE (jacket_number, pdf, formatted_text)
);

CREATE TABLE IF NOT EXISTS bicam_staging_congressional.nominations_staging(
    nomination_id TEXT,
    citation TEXT,
    congress INTEGER,
    number TEXT,
    part TEXT,
    description TEXT,
    received_at TIMESTAMP WITH TIME ZONE,
    authority_date TIMESTAMP WITH TIME ZONE,
    is_privileged BOOLEAN,
    is_civilian BOOLEAN,
    executive_calendar_number TEXT,
    committees_count INTEGER,
    actions_count INTEGER,
    hearings_count INTEGER,
    updated_at TIMESTAMP WITH TIME ZONE,
    _inserted_at TIMESTAMP WITH TIME ZONE,
    UNIQUE (nomination_id, citation, congress, number, part, description, received_at, authority_date, is_privileged, is_civilian, executive_calendar_number, committees_count, actions_count, hearings_count, updated_at)
);

CREATE TABLE IF NOT EXISTS bicam_staging_congressional.nominations_positions_staging(
    nomination_id TEXT,
    ordinal INTEGER,
    position_title TEXT,
    organization TEXT,
    intro_text TEXT,
    nominee_count INTEGER,
    _inserted_at TIMESTAMP WITH TIME ZONE,
    UNIQUE (nomination_id, ordinal, position_title, organization, intro_text, nominee_count)
);

CREATE TABLE IF NOT EXISTS bicam_staging_congressional.nominations_actions_staging(
    nomination_id TEXT,
    action_code TEXT,
    action_date TIMESTAMP WITH TIME ZONE,
    text TEXT,
    type TEXT,
    _inserted_at TIMESTAMP WITH TIME ZONE,
    UNIQUE (nomination_id, action_code, action_date, text, type)
);

CREATE TABLE IF NOT EXISTS bicam_staging_congressional.nominations_actions_committee_codes_staging(
    nomination_id TEXT,
    action_code TEXT,
    action_date TIMESTAMP WITH TIME ZONE,
    text TEXT,
    committee_codes TEXT,
    _inserted_at TIMESTAMP WITH TIME ZONE,
    UNIQUE (nomination_id, action_code, action_date, text, committee_codes)
);

CREATE TABLE IF NOT EXISTS bicam_staging_congressional.nominations_committeeactivities_staging(
    nomination_id TEXT,
    committee_code TEXT,
    committee_name TEXT,
    activity_name TEXT,
    activity_date TIMESTAMP WITH TIME ZONE,
    subcommittee_code TEXT,
    subcommittee_name TEXT,
    subcommittee_activity_name TEXT,
    subcommittee_activity_date TIMESTAMP WITH TIME ZONE,
    _inserted_at TIMESTAMP WITH TIME ZONE,
    UNIQUE (nomination_id, committee_code, committee_name, activity_name, activity_date, subcommittee_code, subcommittee_name, subcommittee_activity_name, subcommittee_activity_date)
);

CREATE TABLE IF NOT EXISTS bicam_staging_congressional.nominations_nominees_staging(
    nomination_id TEXT,
    ordinal INTEGER,
    first_name TEXT,
    middle_name TEXT,
    last_name TEXT,
    prefix TEXT,
    suffix TEXT,
    state TEXT,
    effective_date TIMESTAMP WITH TIME ZONE,
    predecessor_name TEXT,
    corps_code TEXT,
    _inserted_at TIMESTAMP WITH TIME ZONE,
    UNIQUE (nomination_id, ordinal, first_name, middle_name, last_name, prefix, suffix, state, effective_date, predecessor_name, corps_code)
);

CREATE TABLE IF NOT EXISTS bicam_staging_congressional.nominations_hearings_staging(
    nomination_id TEXT,
    hearing_jacketnumber TEXT,
    errata_number TEXT,
    _inserted_at TIMESTAMP WITH TIME ZONE,
    UNIQUE (nomination_id, hearing_jacketnumber, errata_number)
);

CREATE TABLE IF NOT EXISTS bicam_staging_congressional.treaties_staging(
    treaty_id TEXT,
    number TEXT,
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
    _inserted_at TIMESTAMP WITH TIME ZONE,
    UNIQUE (treaty_id, number, suffix, congress_received, congress_considered, topic, transmitted_at, in_force_at, resolution_text, parts_count, actions_count, old_number, old_number_display_name, updated_at)
);

CREATE TABLE IF NOT EXISTS bicam_staging_congressional.treaties_index_terms_staging(
    treaty_id TEXT,
    index_terms TEXT,
    _inserted_at TIMESTAMP WITH TIME ZONE,
    UNIQUE (treaty_id, index_terms)
);

CREATE TABLE IF NOT EXISTS bicam_staging_congressional.treaties_country_parties_staging(
    treaty_id TEXT,
    country_parties TEXT,
    _inserted_at TIMESTAMP WITH TIME ZONE,
    UNIQUE (treaty_id, country_parties)
);

CREATE TABLE IF NOT EXISTS bicam_staging_congressional.treaties_titles_staging(
    treaty_id TEXT,
    title TEXT,
    title_type TEXT,
    _inserted_at TIMESTAMP WITH TIME ZONE,
    UNIQUE (treaty_id, title, title_type)
);

CREATE TABLE IF NOT EXISTS bicam_staging_congressional.treaties_actions_staging(
    treaty_id TEXT,
    action_id TEXT,
    action_code TEXT,
    action_date TIMESTAMP WITH TIME ZONE,
    text TEXT,
    action_type TEXT,
    _inserted_at TIMESTAMP WITH TIME ZONE,
    UNIQUE (treaty_id, action_id, action_code, action_date, text, action_type)
);

CREATE TABLE IF NOT EXISTS bicam_staging_congressional.treaties_actions_committee_codes_staging(
    treaty_id TEXT,
    action_code TEXT,
    action_date TIMESTAMP WITH TIME ZONE,
    text TEXT,
    committee_codes TEXT,
    _inserted_at TIMESTAMP WITH TIME ZONE,
    UNIQUE (treaty_id, action_code, action_date, text, committee_codes)
);

CREATE TABLE IF NOT EXISTS bicam_staging_congressional.congresses_staging(
    number INTEGER,
    name TEXT,
    start_year INTEGER,
    end_year INTEGER,
    updated_at TIMESTAMP WITH TIME ZONE,
    _inserted_at TIMESTAMP WITH TIME ZONE,
    UNIQUE (number, name, start_year, end_year, updated_at)
);

CREATE TABLE IF NOT EXISTS bicam_staging_congressional.congresses_sessions_staging(
    number INTEGER,
    session_number INTEGER,
    chamber TEXT,
    start_date TIMESTAMP WITH TIME ZONE,
    end_date TIMESTAMP WITH TIME ZONE,
    type TEXT,
    _inserted_at TIMESTAMP WITH TIME ZONE,
    UNIQUE (number, session_number, chamber, start_date, end_date, type)
);

DO $$
DECLARE
    current_table_name text;
    current_schema_name text; -- Renamed variable to avoid ambiguity
BEGIN
    FOR current_schema_name IN SELECT schema_name FROM information_schema.schemata WHERE schema_name IN ('bicam_staging_congressional', 'bicam_congressional') LOOP
        FOR current_table_name IN SELECT table_name FROM information_schema.tables WHERE table_schema = current_schema_name LOOP
            EXECUTE format('GRANT SELECT, INSERT, UPDATE, DELETE ON %I.%I TO bicam_pipeline', current_schema_name, current_table_name);
        END LOOP;
    END LOOP;
END$$;

END;
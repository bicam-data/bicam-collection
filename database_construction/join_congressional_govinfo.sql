-- Script to join data from 'congressional' and 'govinfo' schemas into 'bicam' schema

BEGIN;

-- First, insert base data for bills from both sources
INSERT INTO bicam.bills (
    bill_id,
    bill_type,
    bill_number,
    congress,
    title,
    origin_chamber,
    current_chamber,
    version_code,
    introduced_at,
    constitutional_authority_statement,
    is_law,
    is_appropriation,
    is_private,
    policy_area,
    pages,
    actions_count,
    amendments_count,
    committees_count,
    cosponsors_count,
    summaries_count,
    subjects_count,
    titles_count,
    texts_count,
    updated_at
)
SELECT
    COALESCE(c.bill_id, g.bill_id) as bill_id,
    c.bill_type,
    c.bill_number,
    c.congress,
    c.title,
    c.origin_chamber,
    g.current_chamber,
    g.version_code,
    c.introduced_at,
    c.constitutional_authority_statement,
    c.is_law,
    g.is_appropriation,
    g.is_private,
    c.policy_area,
    g.pages,
    c.actions_count,
    c.amendments_count,
    c.committees_count,
    c.cosponsors_count,
    c.summaries_count,
    c.subjects_count,
    c.titles_count,
    c.texts_count,
    GREATEST(c.updated_at, g.last_modified) as updated_at
FROM
    congressional.bills c
FULL OUTER JOIN
    govinfo.bills g ON c.bill_id = g.bill_id;

-- Then insert the metadata
INSERT INTO bicam.bills_metadata (
    bill_id,
    govinfo_package_id,
    su_doc_class_number,
    migrated_doc_id,
    stock_number,
    child_ils_system_id,
    parent_ils_system_id,
    govinfo_collection_code
)
SELECT DISTINCT
    bill_id,
    package_id,
    su_doc_class_number,
    migrated_doc_id,
    stock_number,
    child_ils_system_id,
    parent_ils_system_id,
    collection_code
FROM
    govinfo.bills
WHERE
    bill_id IS NOT NULL;

-- For hearings, we need to handle both direct and granule-based relationships
INSERT INTO bicam.hearings (
    hearing_id,
    hearing_jacketnumber,
    loc_id,
    title,
    congress,
    session,
    chamber,
    is_appropriation,
    hearing_number,
    part_number,
    citation,
    pages,
    updated_at
)
SELECT
    COALESCE(c.hearing_id, g.hearing_id) as hearing_id,
    c.hearing_jacketnumber,
    c.loc_id,
    COALESCE(c.title, g.title) as title,
    COALESCE(c.congress, g.congress) as congress,
    g.session,
    COALESCE(c.chamber, g.chamber) as chamber,
    g.is_appropriation,
    c.hearing_number,
    c.part_number,
    c.citation,
    g.pages,
    GREATEST(c.updated_at, g.last_modified) as updated_at
FROM
    congressional.hearings c
FULL OUTER JOIN
    govinfo.hearings g ON c.hearing_id = g.hearing_id;

-- Insert hearings metadata
INSERT INTO bicam.hearings_metadata (
    hearing_id,
    govinfo_package_id,
    su_doc_class_number,
    migrated_doc_id,
    govinfo_collection_code
)
SELECT DISTINCT
    h.hearing_id,
    h.package_id,
    h.su_doc_class_number,
    h.migrated_doc_id,
    h.collection_code
FROM
    govinfo.hearings h
WHERE
    h.hearing_id IS NOT NULL;

-- For hearings members, we need to join through the granules
INSERT INTO bicam.hearings_members (
    hearing_id,
    bioguide_id
)
SELECT DISTINCT
    hm.hearing_id,
    gm.bioguide_id
FROM
    govinfo.hearings_metadata_granules hg
JOIN
    govinfo.hearings_members gm 
    ON hg.package_id = gm.package_id 
    AND hg.granule_id = gm.granule_id
WHERE
    hg.hearing_id IS NOT NULL 
    AND gm.bioguide_id IS NOT NULL;

-- For members, we need to combine both sources and handle terms through granules
INSERT INTO bicam.members (
    bioguide_id,
    normalized_name,
    direct_order_name,
    inverted_order_name,
    honorific_prefix,
    first_name,
    middle_name,
    last_name,
    suffix,
    nickname,
    party,
    state,
    district,
    birth_year,
    death_year,
    official_url,
    office_address,
    office_city,
    office_district,
    office_zip,
    office_phone,
    sponsored_legislation_count,
    cosponsored_legislation_count,
    depiction_image_url,
    depiction_attribution,
    is_current_member,
    updated_at
)
SELECT
    COALESCE(c.bioguide_id, g.bioguide_id) as bioguide_id,
    c.normalized_name,
    c.direct_order_name,
    c.inverted_order_name,
    c.honorific_prefix,
    c.first_name,
    c.middle_name,
    c.last_name,
    c.suffix,
    c.nickname,
    c.party,
    c.state,
    c.district,
    c.birth_year,
    c.death_year,
    COALESCE(c.official_url, g.official_url),
    c.office_address,
    c.office_city,
    c.office_district,
    c.office_zip,
    c.office_phone,
    c.sponsored_legislation_count,
    c.cosponsored_legislation_count,
    c.depiction_image_url,
    c.depiction_attribution,
    c.is_current_member,
    GREATEST(c.updated_at, g.last_modified) as updated_at
FROM
    congressional.members c
FULL OUTER JOIN
    govinfo.members g ON c.bioguide_id = g.bioguide_id;

-- Insert member terms from both congressional data and govinfo granules
INSERT INTO bicam.members_terms (
    bioguide_id,
    member_type,
    chamber,
    congress,
    start_year,
    end_year,
    state_name,
    state_code,
    district,
    title,
    biography,
    population,
    twitter_url,
    instagram_url,
    facebook_url,
    youtube_url,
    other_url
)
SELECT DISTINCT
    COALESCE(c.bioguide_id, g.bioguide_id) as bioguide_id,
    c.member_type,
    COALESCE(c.chamber, g.chamber) as chamber,
    COALESCE(c.congress, cd.congress) as congress,
    c.start_year,
    c.end_year,
    c.state_name,
    c.state_code,
    c.district,
    g.title,
    g.biography,
    g.population,
    g.twitter_url,
    g.instagram_url,
    g.facebook_url,
    g.youtube_url,
    g.other_url
FROM
    congressional.members_terms c
FULL OUTER JOIN
    govinfo.congresses_directories_granules cdg
    ON c.bioguide_id = cdg.bioguide_id
JOIN
    govinfo.congresses_directories cd
    ON cdg.package_id = cd.package_id
LEFT JOIN
    govinfo.members g
    ON cdg.granule_id = g.granule_id
    AND cdg.package_id = g.package_id;

-- Insert members metadata
INSERT INTO bicam.members_metadata (
    bioguide_id,
    govinfo_granule_id,
    govinfo_package_id,
    gpo_id,
    authority_id
)
SELECT DISTINCT
    m.bioguide_id,
    m.granule_id,
    m.package_id,
    m.gpo_id,
    m.authority_id
FROM
    govinfo.members m
WHERE
    m.bioguide_id IS NOT NULL;

-- Committees data - primarily from congressional with some govinfo metadata
INSERT INTO bicam.committees (
    committee_code,
    name,
    chamber,
    is_subcommittee,
    is_current,
    bills_count,
    reports_count,
    nominations_count,
    updated_at
)
SELECT
    committee_code,
    name,
    chamber,
    is_subcommittee,
    is_current,
    bills_count,
    reports_count,
    nominations_count,
    updated_at
FROM
    congressional.committees;

-- Committee prints - combine data from both sources
INSERT INTO bicam.committeeprints (
    print_id,
    print_jacketnumber,
    congress,
    session,
    chamber,
    title,
    pages,
    print_number,
    citation,
    updated_at
)
SELECT
    COALESCE(c.print_id, g.print_id) as print_id,
    c.print_jacketnumber,
    COALESCE(c.congress, g.congress),
    g.session,
    COALESCE(c.chamber, LOWER(g.chamber)),
    COALESCE(c.title, g.title),
    g.pages,
    c.print_number,
    c.citation,
    GREATEST(c.updated_at, g.last_modified) as updated_at
FROM
    congressional.committeeprints c
FULL OUTER JOIN
    govinfo.committeeprints g 
    ON c.print_id = g.print_id;

-- Committee prints metadata
INSERT INTO bicam.committeeprints_metadata (
    print_id,
    govinfo_package_id,
    su_doc_class_number,
    migrated_doc_id,
    govinfo_collection_code
)
SELECT DISTINCT
    print_id,
    package_id,
    su_doc_class_number,
    migrated_doc_id,
    collection_code
FROM
    govinfo.committeeprints
WHERE
    print_id IS NOT NULL;

-- Committee reports - combine data from both sources
INSERT INTO bicam.committeereports (
    report_id,
    report_type,
    report_number,
    report_part,
    congress,
    session,
    title,
    subtitle,
    chamber,
    citation,
    is_conference_report,
    issued_at,
    pages,
    texts_count,
    updated_at
)
SELECT
    COALESCE(c.report_id, g.report_id) as report_id,
    c.report_type,
    c.report_number,
    c.report_part,
    COALESCE(c.congress, g.congress),
    g.session,
    COALESCE(c.title, g.title),
    g.subtitle,
    COALESCE(c.chamber, LOWER(g.chamber)),
    c.citation,
    c.is_conference_report,
    COALESCE(c.issued_at, g.date_issued),
    g.pages,
    c.texts_count,
    GREATEST(c.updated_at, g.last_modified) as updated_at
FROM
    congressional.committeereports c
FULL OUTER JOIN
    govinfo.committeereports g 
    ON c.report_id = g.report_id;

-- Committee reports metadata
INSERT INTO bicam.committeereports_metadata (
    report_id,
    govinfo_package_id,
    su_doc_class_number,
    migrated_doc_id,
    govinfo_collection_code
)
SELECT DISTINCT
    report_id,
    package_id,
    su_doc_class_number,
    migrated_doc_id,
    collection_code
FROM
    govinfo.committeereports
WHERE
    report_id IS NOT NULL;

-- Treaties - combine data from both sources
INSERT INTO bicam.treaties (
    treaty_id,
    treaty_number,
    suffix,
    congress_received,
    congress_received_session,
    congress_considered,
    topic,
    transmitted_at,
    in_force_at,
    resolution_text,
    summary,
    pages,
    parts_count,
    actions_count,
    old_number,
    old_number_display_name,
    updated_at
)
SELECT
    COALESCE(c.treaty_id, g.treaty_id) as treaty_id,
    c.treaty_number,
    c.suffix,
    c.congress_received,
    g.session as congress_received_session,
    c.congress_considered,
    c.topic,
    c.transmitted_at,
    c.in_force_at,
    c.resolution_text,
    COALESCE(c.summary, g.summary),
    g.pages,
    c.parts_count,
    c.actions_count,
    c.old_number,
    c.old_number_display_name,
    GREATEST(c.updated_at, g.last_modified) as updated_at
FROM
    congressional.treaties c
FULL OUTER JOIN
    govinfo.treaties g 
    ON c.treaty_id = g.treaty_id;

-- Treaties metadata
INSERT INTO bicam.treaties_metadata (
    treaty_id,
    govinfo_package_id,
    su_doc_class_number,
    migrated_doc_id,
    govinfo_collection_code
)
SELECT DISTINCT
    treaty_id,
    package_id,
    su_doc_class_number,
    migrated_doc_id,
    collection_code
FROM
    govinfo.treaties
WHERE
    treaty_id IS NOT NULL;

-- Nominations - primarily from congressional with some govinfo enrichment
INSERT INTO bicam.nominations (
    nomination_id,
    nomination_number,
    part_number,
    congress,
    description,
    is_privileged,
    is_civilian,
    received_at,
    authority_date,
    executive_calendar_number,
    citation,
    committees_count,
    actions_count,
    updated_at
)
SELECT
    COALESCE(c.nomination_id, g.nomination_id) as nomination_id,
    c.nomination_number,
    c.part_number,
    c.congress,
    c.description,
    c.is_privileged,
    c.is_civilian,
    c.received_at,
    c.authority_date,
    c.executive_calendar_number,
    c.citation,
    c.committees_count,
    c.actions_count,
    c.updated_at
FROM
    congressional.nominations c
FULL OUTER JOIN
    govinfo.nominations g 
    ON c.nomination_id = g.nomination_id;

-- Congresses - base data
INSERT INTO bicam.congresses (
    congress_number,
    name,
    start_year,
    end_year,
    updated_at
)
SELECT
    c.congress_number,
    c.name,
    c.start_year,
    c.end_year,
    c.updated_at
FROM
    congressional.congresses c;

-- Congress directories
INSERT INTO bicam.congresses_directories (
    congress_number,
    title,
    issued_at,
    txt_url,
    pdf_url,
    govinfo_package_id,
    su_doc_class_number,
    migrated_doc_id,
    ils_system_id,
    govinfo_collection_code,
    government_author1,
    government_author2,
    publisher,
    last_modified
)
SELECT
    g.congress,
    g.title,
    g.date_issued,
    g.txt_url,
    g.pdf_url,
    g.package_id,
    g.su_doc_class_number,
    g.migrated_doc_id,
    g.ils_system_id,
    g.collection_code,
    g.government_author1,
    g.government_author2,
    g.publisher,
    g.last_modified
FROM
    govinfo.congressional_directories g;

-- Committee prints committees - combine both sources
INSERT INTO bicam.committeeprints_committees (
    print_id,
    committee_code
)
SELECT DISTINCT
    COALESCE(c.print_id, g.print_id) as print_id,
    COALESCE(c.committee_code, g.committee_code) as committee_code
FROM
    congressional.committeeprints_committees c
FULL OUTER JOIN
    govinfo.committeeprints_committees g
    ON c.print_id = g.print_id 
    AND c.committee_code = g.committee_code
WHERE
    COALESCE(c.print_id, g.print_id) IS NOT NULL
    AND COALESCE(c.committee_code, g.committee_code) IS NOT NULL;

-- Committee prints associated bills - combine both sources
INSERT INTO bicam.committeeprints_associated_bills (
    print_id,
    bill_id
)
SELECT DISTINCT
    COALESCE(c.print_id, g.print_id) as print_id,
    COALESCE(c.bill_id, g.bill_id) as bill_id
FROM
    congressional.committeeprints_associated_bills c
FULL OUTER JOIN
    govinfo.committeeprints_bills g
    ON c.print_id = g.print_id 
    AND c.bill_id = g.bill_id
WHERE
    COALESCE(c.print_id, g.print_id) IS NOT NULL
    AND COALESCE(c.bill_id, g.bill_id) IS NOT NULL;

-- Committee reports committees - combine both sources
INSERT INTO bicam.committeereports_committees (
    report_id,
    committee_code
)
SELECT DISTINCT
    COALESCE(c.report_id, g.report_id) as report_id,
    COALESCE(c.committee_code, g.committee_code) as committee_code
FROM
    congressional.committees_committeereports c
FULL OUTER JOIN
    govinfo.committeereports_committees g
    ON c.report_id = g.report_id 
    AND c.committee_code = g.committee_code
WHERE
    COALESCE(c.report_id, g.report_id) IS NOT NULL
    AND COALESCE(c.committee_code, g.committee_code) IS NOT NULL;

-- Committee reports associated bills - combine both sources
INSERT INTO bicam.committeereports_associated_bills (
    report_id,
    bill_id
)
SELECT DISTINCT
    COALESCE(c.report_id, g.report_id) as report_id,
    COALESCE(c.bill_id, g.bill_id) as bill_id
FROM
    congressional.committeereports_associated_bills c
FULL OUTER JOIN
    govinfo.committeereports_bills g
    ON c.report_id = g.report_id 
    AND c.bill_id = g.bill_id
WHERE
    COALESCE(c.report_id, g.report_id) IS NOT NULL
    AND COALESCE(c.bill_id, g.bill_id) IS NOT NULL;

-- Hearings committees - combine both sources
INSERT INTO bicam.hearings_committees (
    hearing_id,
    committee_code,
    committee_name
)
SELECT DISTINCT
    COALESCE(c.hearing_id, g.hearing_id) as hearing_id,
    COALESCE(c.committee_code, g.committee_code) as committee_code,
    COALESCE(c.committee_name, g.committee_name) as committee_name
FROM
    congressional.hearings_committees c
FULL OUTER JOIN
    govinfo.hearings_committees g
    ON c.hearing_id = g.hearing_id 
    AND c.committee_code = g.committee_code
WHERE
    COALESCE(c.hearing_id, g.hearing_id) IS NOT NULL
    AND COALESCE(c.committee_code, g.committee_code) IS NOT NULL;

-- Add other related tables from congressional schema

-- Bills actions
INSERT INTO bicam.bills_actions (
    action_id,
    bill_id,
    action_code,
    action_date,
    text,
    action_type,
    source_system,
    source_system_code,
    calendar,
    calendar_number
)
SELECT
    action_id,
    bill_id,
    action_code,
    action_date,
    text,
    action_type,
    source_system,
    source_system_code,
    calendar,
    calendar_number
FROM
    congressional.bills_actions;

-- Bills actions committees
INSERT INTO bicam.bills_actions_committees (
    action_id,
    bill_id,
    committee_code
)
SELECT
    action_id,
    bill_id,
    committee_code
FROM
    congressional.bills_actions_committees;

-- Bills cosponsors
INSERT INTO bicam.bills_cosponsors (
    bill_id,
    bioguide_id
)
SELECT
    bill_id,
    bioguide_id
FROM
    congressional.bills_cosponsors;

-- Bills sponsors
INSERT INTO bicam.bills_sponsors (
    bill_id,
    bioguide_id
)
SELECT
    bill_id,
    bioguide_id
FROM
    congressional.bills_sponsors;

-- Bills subjects
INSERT INTO bicam.bills_subjects (
    bill_id,
    subject,
    updated_at
)
SELECT
    bill_id,
    subject,
    updated_at
FROM
    congressional.bills_subjects;

-- Bills summaries
INSERT INTO bicam.bills_summaries (
    bill_id,
    action_date,
    action_desc,
    text,
    version_code
)
SELECT
    bill_id,
    action_date,
    action_desc,
    text,
    version_code
FROM
    congressional.bills_summaries;

-- Bills titles
INSERT INTO bicam.bills_titles (
    bill_id,
    title,
    title_type,
    bill_text_version_code,
    bill_text_version_name,
    chamber,
    title_type_code
)
SELECT
    bill_id,
    title,
    title_type,
    bill_text_version_code,
    bill_text_version_name,
    chamber,
    title_type_code
FROM
    congressional.bills_titles;

-- Committee subcommittees
INSERT INTO bicam.committees_subcommittees (
    committee_code,
    subcommittee_code
)
SELECT
    committee_code,
    subcommittee_code
FROM
    congressional.committees_subcommittees;

-- Members leadership roles
INSERT INTO bicam.members_leadership_roles (
    bioguide_id,
    role,
    congress,
    chamber,
    is_current
)
SELECT
    bioguide_id,
    role,
    congress,
    chamber,
    is_current
FROM
    congressional.members_leadership_roles;

-- Members party history
INSERT INTO bicam.members_party_history (
    bioguide_id,
    party_code,
    party_name,
    start_year,
    end_year
)
SELECT
    bioguide_id,
    party_code,
    party_name,
    start_year,
    end_year
FROM
    congressional.members_party_history;

-- Treaties actions
INSERT INTO bicam.treaties_actions (
    action_id,
    treaty_id,
    action_code,
    action_date,
    text,
    action_type
)
SELECT
    action_id,
    treaty_id,
    action_code,
    action_date,
    text,
    action_type
FROM
    congressional.treaties_actions;

-- Treaties actions committees
INSERT INTO bicam.treaties_actions_committees (
    action_id,
    treaty_id,
    committee_code
)
SELECT
    action_id,
    treaty_id,
    committee_code
FROM
    congressional.treaties_actions_committees;

-- Treaties country parties
INSERT INTO bicam.treaties_country_parties (
    treaty_id,
    country
)
SELECT
    treaty_id,
    country
FROM
    congressional.treaties_country_parties;

-- Treaties index terms
INSERT INTO bicam.treaties_index_terms (
    treaty_id,
    index_term
)
SELECT
    treaty_id,
    index_term
FROM
    congressional.treaties_index_terms;

-- Treaties titles
INSERT INTO bicam.treaties_titles (
    treaty_id,
    title,
    title_type
)
SELECT
    treaty_id,
    title,
    title_type
FROM
    congressional.treaties_titles;

-- Nominations actions
INSERT INTO bicam.nominations_actions (
    action_id,
    nomination_id,
    action_code,
    action_type,
    action_date,
    text
)
SELECT
    action_id,
    nomination_id,
    action_code,
    action_type,
    action_date,
    text
FROM
    congressional.nominations_actions;

-- Nominations positions
INSERT INTO bicam.nominations_positions (
    nomination_id,
    ordinal,
    position_title,
    organization,
    intro_text,
    nominee_count
)
SELECT
    nomination_id,
    ordinal,
    position_title,
    organization,
    intro_text,
    nominee_count
FROM
    congressional.nominations_positions;

-- Nominations actions committees
INSERT INTO bicam.nominations_actions_committees (
    action_id,
    nomination_id,
    committee_code
)
SELECT
    action_id,
    nomination_id,
    committee_code
FROM
    congressional.nominations_actions_committees;

-- Nominations committee activities
INSERT INTO bicam.nominations_committeeactivities (
    nomination_id,
    committee_code,
    activity_name,
    activity_date
)
SELECT
    nomination_id,
    committee_code,
    activity_name,
    activity_date
FROM
    congressional.nominations_committeeactivities;

-- Nominations nominees
INSERT INTO bicam.nominations_nominees (
    nomination_id,
    ordinal,
    first_name,
    middle_name,
    last_name,
    prefix,
    suffix,
    state,
    effective_date,
    predecessor_name,
    corps_code
)
SELECT
    nomination_id,
    ordinal,
    first_name,
    middle_name,
    last_name,
    prefix,
    suffix,
    state,
    effective_date,
    predecessor_name,
    corps_code
FROM
    congressional.nominations_nominees;

-- Committee meetings - combine data from both sources
INSERT INTO bicam.committeemeetings (
    meeting_id,
    title,
    meeting_type,
    chamber,
    congress,
    date,
    room,
    street_address,
    building,
    city,
    state,
    zip_code,
    meeting_status,
    updated_at
)
SELECT
    meeting_id,
    title,
    meeting_type,
    chamber,
    congress,
    date,
    room,
    street_address,
    building,
    city,
    state,
    zip_code,
    meeting_status,
    updated_at
FROM
    congressional.committeemeetings;

-- Committee meetings committees
INSERT INTO bicam.committeemeetings_committees (
    meeting_id,
    committee_code
)
SELECT
    meeting_id,
    committee_code
FROM
    congressional.committeemeetings_committees;

-- Committee meetings documents
INSERT INTO bicam.committeemeetings_meeting_documents (
    meeting_id,
    name,
    document_type,
    description,
    url
)
SELECT
    meeting_id,
    name,
    document_type,
    description,
    url
FROM
    congressional.committeemeetings_meeting_documents;

-- Committee meetings witness documents
INSERT INTO bicam.committeemeetings_witness_documents (
    meeting_id,
    document_type,
    url
)
SELECT
    meeting_id,
    document_type,
    url
FROM
    congressional.committeemeetings_witness_documents;

-- Committee meetings witnesses
INSERT INTO bicam.committeemeetings_witnesses (
    meeting_id,
    name,
    position,
    organization
)
SELECT
    meeting_id,
    name,
    position,
    organization
FROM
    congressional.committeemeetings_witnesses;

-- Bills texts - from congressional only
INSERT INTO bicam.bills_texts (
    bill_id,
    date,
    type,
    raw_text,
    formatted_text,
    pdf,
    xml
)
SELECT
    bill_id,
    date,
    type,
    raw_text,
    formatted_text,
    pdf,
    xml
FROM
    congressional.bills_texts;

-- Committee prints texts - from congressional only
INSERT INTO bicam.committeeprints_texts (
    print_id,
    raw_text,
    formatted_text,
    pdf,
    html,
    xml,
    png
)
SELECT
    print_id,
    raw_text,
    formatted_text,
    pdf,
    html,
    xml,
    png
FROM
    congressional.committeeprints_texts;

-- Committee reports texts - from congressional only
INSERT INTO bicam.committeereports_texts (
    report_id,
    raw_text,
    formatted_text,
    formatted_text_is_errata,
    pdf,
    pdf_is_errata
)
SELECT
    report_id,
    raw_text,
    formatted_text,
    false as formatted_text_is_errata,
    pdf,
    false as pdf_is_errata
FROM
    congressional.committeereports_texts;

-- Hearings texts - from congressional only
INSERT INTO bicam.hearings_texts (
    hearing_id,
    raw_text,
    pdf,
    formatted_text
)
SELECT
    hearing_id,
    raw_text,
    pdf,
    formatted_text
FROM
    congressional.hearings_texts;

-- Bills notes
INSERT INTO bicam.bills_notes (
    bill_id,
    note_number,
    note_text,
    updated_at
)
SELECT
    bill_id,
    note_number,
    note_text,
    updated_at
FROM
    congressional.bills_notes;

-- Bills notes links
INSERT INTO bicam.bills_notes_links (
    bill_id,
    note_number,
    link_name,
    link_url,
    updated_at
)
SELECT
    bill_id,
    note_number,
    link_name,
    link_url,
    updated_at
FROM
    congressional.bills_notes_links;

-- Bills reference codes
INSERT INTO bicam.bills_reference_codes (
    bill_code_id,
    bill_id,
    reference_code
)
SELECT
    bill_code_id,
    bill_id,
    reference_code
FROM
    congressional.bills_reference_codes;

-- Bills reference codes sections
INSERT INTO bicam.bills_reference_codes_sections (
    bill_code_id,
    code_section
)
SELECT
    bill_code_id,
    code_section
FROM
    govinfo.bills_reference_codes_sections;

-- Bills reference laws
INSERT INTO bicam.bills_reference_laws (
    bill_id,
    law_id,
    law_type
)
SELECT
    bill_id,
    law_id,
    law_type
FROM
    govinfo.bills_reference_laws;

-- Bills reference statutes
INSERT INTO bicam.bills_reference_statutes (
    bill_statute_id,
    bill_id,
    reference_statute
)
SELECT
    bill_statute_id,
    bill_id,
    reference_statute
FROM
    govinfo.bills_reference_statutes;

-- Bills reference statutes pages
INSERT INTO bicam.bills_reference_statutes_pages (
    bill_statute_id,
    page
)
SELECT
    bill_statute_id,
    page
FROM
    govinfo.bills_reference_statutes_pages;

-- Committee history
INSERT INTO bicam.committees_history (
    committee_code,
    name,
    loc_name,
    started_at,
    ended_at,
    committee_type,
    establishing_authority,
    su_doc_class_number,
    nara_id,
    loc_linked_data_id,
    updated_at
)
SELECT
    committee_code,
    name,
    loc_name,
    started_at,
    ended_at,
    committee_type,
    establishing_authority,
    su_doc_class_number,
    nara_id,
    loc_linked_data_id,
    updated_at
FROM
    congressional.committees_history;

-- Hearings dates
INSERT INTO bicam.hearings_dates (
    hearing_id,
    hearing_date
)
SELECT
    hearing_id,
    hearing_date
FROM
    congressional.hearings_dates;

-- Hearings witnesses
INSERT INTO bicam.hearings_witnesses (
    hearing_id,
    witness
)
SELECT
    hearing_id,
    witness
FROM
    govinfo.hearings_witnesses;

-- Hearings bills
INSERT INTO bicam.hearings_bills (
    hearing_id,
    bill_id
)
SELECT
    hearing_id,
    bill_id
FROM
    govinfo.hearings_reference_bills;

-- Committee meetings associated bills
INSERT INTO bicam.committeemeetings_associated_bills (
    meeting_id,
    bill_id
)
SELECT
    meeting_id,
    bill_id
FROM
    congressional.committeemeetings_associated_bills;

-- Committee meetings associated treaties
INSERT INTO bicam.committeemeetings_associated_treaties (
    meeting_id,
    treaty_id
)
SELECT
    meeting_id,
    treaty_id
FROM
    congressional.committeemeetings_associated_treaties;

-- Committee meetings associated nominations
INSERT INTO bicam.committeemeetings_associated_nominations (
    meeting_id,
    nomination_id
)
SELECT
    meeting_id,
    nomination_id
FROM
    congressional.committeemeetings_associated_nominations;

-- Committee meetings associated hearings
INSERT INTO bicam.committeemeetings_associated_hearings (
    meeting_id,
    hearing_id
)
SELECT
    meeting_id,
    hearing_id
FROM
    congressional.committeemeetings_associated_hearings;

-- Bills CBO cost estimates
INSERT INTO bicam.bills_cbocostestimates (
    bill_id,
    description,
    pub_date,
    title,
    url
)
SELECT
    bill_id,
    description,
    pub_date,
    title,
    url
FROM
    congressional.bills_cbocostestimates;

-- Bills related bills
INSERT INTO bicam.bills_related_bills (
    bill_id,
    related_bill_id,
    identification_entity
)
SELECT
    bill_id,
    related_bill_id,
    identification_entity
FROM
    congressional.bills_related_bills;

-- Bills actions recorded votes
INSERT INTO bicam.bills_actions_recorded_votes (
    action_id,
    bill_id,
    chamber,
    congress,
    date,
    roll_number,
    session,
    url
)
SELECT
    action_id,
    bill_id,
    chamber,
    congress,
    date,
    roll_number,
    session,
    url
FROM
    congressional.bills_actions_recorded_votes;

-- Congresses sessions
INSERT INTO bicam.congresses_sessions (
    congress_number,
    session,
    chamber,
    type,
    start_date,
    end_date
)
SELECT
    congress_number,
    session,
    chamber,
    type,
    start_date,
    end_date
FROM
    congressional.congresses_sessions;

-- Congresses directories ISBN
INSERT INTO bicam.congresses_directories_isbn (
    congress_number,
    govinfo_package_id,
    isbn
)
SELECT
    cd.congress,
    cd.package_id,
    i.isbn
FROM
    govinfo.congressional_directories cd
JOIN
    govinfo.congressional_directories_isbn i
    ON cd.package_id = i.package_id;

-- Nominations associated hearings
INSERT INTO bicam.nominations_associated_hearings (
    nomination_id,
    hearing_id
)
SELECT
    nomination_id,
    hearing_id
FROM
    congressional.nominations_associated_hearings;

-- Treaties metadata granules
INSERT INTO bicam.treaties_metadata_granules (
    treaty_id,
    govinfo_package_id,
    govinfo_granule_id
)
SELECT DISTINCT
    t.treaty_id,
    t.package_id,
    g.granule_id
FROM
    govinfo.treaties t
JOIN
    govinfo.treaties_granules g
    ON t.package_id = g.package_id
WHERE
    t.treaty_id IS NOT NULL;

-- Hearings metadata granules
INSERT INTO bicam.hearings_metadata_granules (
    hearing_id,
    govinfo_package_id,
    govinfo_granule_id
)
SELECT DISTINCT
    h.hearing_id,
    h.package_id,
    g.granule_id
FROM
    govinfo.hearings h
JOIN
    govinfo.hearings_granules g
    ON h.package_id = g.package_id
WHERE
    h.hearing_id IS NOT NULL;

-- Add missing bills actions committees
INSERT INTO bicam.bills_actions_committees (
    action_id,
    bill_id,
    committee_code
)
SELECT
    action_id,
    bill_id,
    committee_code
FROM
    congressional.bills_actions_committees;

-- Add missing committees bills
INSERT INTO bicam.committees_bills (
    committee_code,
    bill_id,
    relationship_type,
    committee_action_date,
    updated_at
)
SELECT DISTINCT
    COALESCE(c.committee_code, g.committee_code) as committee_code,
    COALESCE(c.bill_id, g.bill_id) as bill_id,
    c.relationship_type,
    c.committee_action_date,
    c.updated_at
FROM
    congressional.committees_bills c
FULL OUTER JOIN
    govinfo.committees_bills g
    ON c.committee_code = g.committee_code
    AND c.bill_id = g.bill_id;

-- Add missing bills laws
INSERT INTO bicam.bills_laws (
    bill_id,
    law_id,
    law_number,
    law_type
)
SELECT
    bill_id,
    law_id,
    law_number,
    law_type
FROM
    congressional.bills_laws;

-- Add missing committee reports members
INSERT INTO bicam.committeereports_members (
    report_id,
    bioguide_id
)
SELECT
    report_id,
    bioguide_id
FROM
    congressional.committeereports_members;

-- Add missing committee reports associated treaties
INSERT INTO bicam.committeereports_associated_treaties (
    report_id,
    treaty_id
)
SELECT
    report_id,
    treaty_id
FROM
    congressional.committeereports_associated_treaties;

-- Add amendments actions committees
INSERT INTO bicam.amendments_actions_committees (
    action_id,
    amendment_id,
    committee_code
)
SELECT
    action_id,
    amendment_id,
    committee_code
FROM
    congressional.amendments_actions_committees;

-- Add amendments actions committees members
INSERT INTO bicam.amendments_actions_committees_members (
    action_id,
    amendment_id,
    committee_code,
    bioguide_id
)
SELECT
    action_id,
    amendment_id,
    committee_code,
    bioguide_id
FROM
    congressional.amendments_actions_committees_members;

END;

COMMIT; 
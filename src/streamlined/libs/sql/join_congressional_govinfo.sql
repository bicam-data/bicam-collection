-- Script to join data from 'congressional' and 'govinfo' schemas into 'bicam' schema

DO $$
BEGIN
    -- Check if target schema exists
    IF NOT EXISTS (SELECT 1 FROM information_schema.schemata WHERE schema_name = 'bicam') THEN
        RAISE EXCEPTION 'Target schema bicam does not exist. Please create it first.';
    END IF;

    -- Check if source schemas exist
    IF NOT EXISTS (SELECT 1 FROM information_schema.schemata WHERE schema_name = 'staging_congressional') THEN
        RAISE EXCEPTION 'Source schema staging_congressional does not exist.';
    END IF;

    IF NOT EXISTS (SELECT 1 FROM information_schema.schemata WHERE schema_name = 'staging_govinfo') THEN
        RAISE EXCEPTION 'Source schema staging_govinfo does not exist.';
    END IF;

    -- Start transaction
    BEGIN
        RAISE NOTICE 'Starting data join from staging schemas to bicam...';

        -- Bills
        RAISE NOTICE 'Processing bills...';


UPDATE staging_govinfo.hearings
   SET hearing_id = REGEXP_REPLACE(
        package_id,
        '^(?:GPO-)?CHRG-(\d+)([a-z]+)(\d.*)',
        '\2\3-\1'
    )
WHERE hearing_id IS NULL AND package_id ~ '\d';

UPDATE staging_govinfo.hearings AS h
    SET is_appropriation = sh.is_appropriation::boolean
FROM (
    SELECT
        payload ->> 'packageId' as package_id,
        payload ->> 'isAppropriation' as is_appropriation
    FROM bicam_raw_govinfo.hearingpackages_granules_raw
) AS sh
WHERE h.package_id = sh.package_id
AND h.is_appropriation IS NULL;

-- INSERT INTO staging_congressional.committeemeetings_meeting_documents (meeting_id, description, document_type)
-- SELECT scm.meeting_id, scm.description, document_type
-- FROM _staging_congressional._committeemeetings_meeting_documents AS scm;

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
SELECT DISTINCT ON (COALESCE(c.bill_id, g.bill_id))
    COALESCE(c.bill_id, g.bill_id) as bill_id,
    COALESCE(c.bill_type, regexp_replace(split_part(g.bill_id, '-', 1), '\d+', '')),
    COALESCE(c.bill_number::int, regexp_replace(split_part(g.bill_id, '-', 1), '\D+', '')::int),
    c.congress,
    c.title,
    COALESCE(c.origin_chamber, g.origin_chamber),
    g.current_chamber,
    g.bill_version,
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
    staging_congressional.bills c
FULL OUTER JOIN
    staging_govinfo.bills g ON c.bill_id = g.bill_id
WHERE COALESCE(c.bill_id, g.bill_id) IS NOT NULL
ORDER BY COALESCE(c.bill_id, g.bill_id), c.updated_at DESC NULLS LAST, g.last_modified DESC NULLS LAST
ON CONFLICT (bill_id) DO UPDATE SET
    bill_type = EXCLUDED.bill_type,
    bill_number = EXCLUDED.bill_number,
    congress = EXCLUDED.congress,
    title = EXCLUDED.title,
    origin_chamber = EXCLUDED.origin_chamber,
    current_chamber = EXCLUDED.current_chamber,
    version_code = EXCLUDED.version_code,
    introduced_at = EXCLUDED.introduced_at,
    constitutional_authority_statement = EXCLUDED.constitutional_authority_statement,
    is_law = EXCLUDED.is_law,
    is_appropriation = EXCLUDED.is_appropriation,
    is_private = EXCLUDED.is_private,
    policy_area = EXCLUDED.policy_area,
    pages = EXCLUDED.pages,
    actions_count = EXCLUDED.actions_count,
    amendments_count = EXCLUDED.amendments_count,
    committees_count = EXCLUDED.committees_count,
    cosponsors_count = EXCLUDED.cosponsors_count,
    summaries_count = EXCLUDED.summaries_count,
    subjects_count = EXCLUDED.subjects_count,
    titles_count = EXCLUDED.titles_count,
    texts_count = EXCLUDED.texts_count,
    updated_at = EXCLUDED.updated_at;

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
    staging_govinfo.bills
WHERE
    bill_id IS NOT NULL
ON CONFLICT (bill_id, govinfo_package_id) DO UPDATE SET
    su_doc_class_number = EXCLUDED.su_doc_class_number,
    migrated_doc_id = EXCLUDED.migrated_doc_id,
    stock_number = EXCLUDED.stock_number,
    child_ils_system_id = EXCLUDED.child_ils_system_id,
    parent_ils_system_id = EXCLUDED.parent_ils_system_id,
    govinfo_collection_code = EXCLUDED.govinfo_collection_code;

INSERT INTO bicam.amendments (
    amendment_id,
    amendment_type,
    amendment_number,
    congress,
    chamber,
    purpose,
    description,
    proposed_at,
    submitted_at,
    is_bill_amendment,
    is_treaty_amendment,
    is_amendment_amendment,
    notes,
    actions_count,
    cosponsors_count,
    amendments_to_amendment_count,
    updated_at
)
SELECT
    c.amendment_id,
    lower(c.amendment_type),
    c.amendment_number,
    c.congress,
    c.chamber,
    c.purpose,
    c.description,
    c.proposed_at,
    c.submitted_at,
    c.is_bill_amendment,
    c.is_treaty_amendment,
    c.is_amendment_amendment,
    c.notes,
    c.actions_count,
    c.cosponsors_count,
    c.amendments_to_amendment_count,
    c.updated_at
FROM
    staging_congressional.amendments c
ON CONFLICT (amendment_id) DO UPDATE SET
    amendment_type = EXCLUDED.amendment_type,
    amendment_number = EXCLUDED.amendment_number,
    congress = EXCLUDED.congress,
    chamber = EXCLUDED.chamber,
    purpose = EXCLUDED.purpose,
    description = EXCLUDED.description,
    proposed_at = EXCLUDED.proposed_at,
    submitted_at = EXCLUDED.submitted_at,
    is_bill_amendment = EXCLUDED.is_bill_amendment,
    is_treaty_amendment = EXCLUDED.is_treaty_amendment,
    is_amendment_amendment = EXCLUDED.is_amendment_amendment,
    notes = EXCLUDED.notes,
    actions_count = EXCLUDED.actions_count,
    cosponsors_count = EXCLUDED.cosponsors_count,
    amendments_to_amendment_count = EXCLUDED.amendments_to_amendment_count,
    updated_at = EXCLUDED.updated_at;

INSERT INTO bicam.amendments_actions (
    action_id,
    amendment_id,
    action_code,
    action_date,
    text,
    action_type,
    source_system,
    source_system_code
)
SELECT
    action_id,
    amendment_id,
    CASE WHEN action_code = '-99' THEN NULL ELSE action_code END AS action_code,
    action_date,
    text,
    action_type,
    source_system,
    source_system_code
FROM
    staging_congressional.amendments_actions
ON CONFLICT (action_id) DO UPDATE SET
    amendment_id = EXCLUDED.amendment_id,
    action_code = EXCLUDED.action_code,
    action_date = EXCLUDED.action_date,
    text = EXCLUDED.text,
    action_type = EXCLUDED.action_type,
    source_system = EXCLUDED.source_system,
    source_system_code = EXCLUDED.source_system_code;

INSERT INTO bicam.amendments_actions_recorded_votes (
    action_id,
    amendment_id,
    chamber,
    congress,
    date,
    roll_number,
    session,
    url
)
SELECT
    action_id,
    amendment_id,
    chamber,
    congress,
    date,
    roll_number,
    session,
    url
FROM
    staging_congressional.amendments_actions_recorded_votes
ON CONFLICT (action_id, amendment_id, url) DO UPDATE SET
    chamber = EXCLUDED.chamber,
    congress = EXCLUDED.congress,
    date = EXCLUDED.date,
    roll_number = EXCLUDED.roll_number,
    session = EXCLUDED.session;

INSERT INTO bicam.amendments_amended_amendments (
    amendment_id,
    amended_amendment_id
)
SELECT
    amendment_id,
    amended_amendment_id
FROM staging_congressional.amendments_amended_amendments
ON CONFLICT (amendment_id, amended_amendment_id) DO UPDATE SET
    amendment_id = EXCLUDED.amendment_id,
    amended_amendment_id = EXCLUDED.amended_amendment_id;


INSERT INTO bicam.amendments_texts (
    amendment_id,
    date,
    type,
    raw_text,
    pdf,
    html
)
SELECT
    amendment_id,
    date,
    type,
    raw_text,
    pdf,
    html
FROM staging_congressional.amendments_texts
ON CONFLICT (amendment_id, date, type) DO UPDATE SET
    raw_text = EXCLUDED.raw_text,
    pdf = EXCLUDED.pdf,
    html = EXCLUDED.html;

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
WITH congressional_with_parts AS (
    SELECT
        CASE
            WHEN part_number IS NOT NULL THEN
                SPLIT_PART(hearing_id, '-', 1) || 'p' || trim_scale(part_number::numeric) || '-' || SPLIT_PART(hearing_id, '-', 2)
            ELSE hearing_id
        END as part_hearing_id,
        *
    FROM staging_congressional.hearings
)
SELECT DISTINCT ON (COALESCE(c.part_hearing_id, g.hearing_id, c.hearing_id))
    COALESCE(c.part_hearing_id, g.hearing_id, c.hearing_id) as hearing_id,
    c.hearing_jacketnumber,
    c.loc_id,
    COALESCE(c.title, g.title) as title,
    COALESCE(c.congress, g.congress) as congress,
    g.session,
    LOWER(COALESCE(c.chamber, g.chamber)) as chamber,
    (sh.payload->> 'isAppropriation')::boolean AS is_appropriation,
    c.hearing_number,
    trim_scale(c.part_number::numeric) AS part_number,
    c.citation,
    g.pages,
    GREATEST(c.updated_at, g.last_modified) as updated_at
FROM
    congressional_with_parts c
FULL JOIN
    staging_govinfo.hearings g ON c.part_hearing_id = g.hearing_id
LEFT JOIN bicam_raw_govinfo.hearingpackages_granules_raw sh
    ON g.package_id = (sh.payload ->> 'packageId')
WHERE COALESCE(c.part_hearing_id, g.hearing_id, c.hearing_id) IS NOT NULL
    AND (g.package_id ~ '\d' OR g.package_id IS NULL)
ORDER BY COALESCE(c.part_hearing_id, g.hearing_id, c.hearing_id), c.updated_at DESC NULLS LAST, g.last_modified DESC NULLS LAST
ON CONFLICT (hearing_id) DO UPDATE SET
    hearing_jacketnumber = EXCLUDED.hearing_jacketnumber,
    loc_id = EXCLUDED.loc_id,
    title = EXCLUDED.title,
    congress = EXCLUDED.congress,
    session = EXCLUDED.session,
    chamber = EXCLUDED.chamber,
    is_appropriation = EXCLUDED.is_appropriation,
    hearing_number = EXCLUDED.hearing_number,
    part_number = EXCLUDED.part_number,
    citation = EXCLUDED.citation,
    pages = EXCLUDED.pages,
    updated_at = EXCLUDED.updated_at;

-- Insert hearings metadata
INSERT INTO bicam.hearings_metadata (
    hearing_id,
    govinfo_package_id,
    su_doc_class_number,
    migrated_doc_id,
    govinfo_collection_code
)
WITH congressional_with_parts AS (
    SELECT
        CASE
            WHEN part_number IS NOT NULL THEN
                SPLIT_PART(hearing_id, '-', 1) || 'p' || trim_scale(part_number::numeric) || '-' || SPLIT_PART(hearing_id, '-', 2)
            ELSE hearing_id
        END as part_hearing_id,
        *
    FROM staging_congressional.hearings
)
SELECT DISTINCT
    COALESCE(c.part_hearing_id, h.hearing_id) as
    hearing_id,
    h.package_id,
    h.su_doc_class_number,
    h.migrated_doc_id,
    h.collection_code
FROM
    staging_govinfo.hearings h
LEFT JOIN congressional_with_parts c
    ON h.hearing_id = c.part_hearing_id
WHERE h.package_id ~ '\d'
ON CONFLICT (hearing_id, govinfo_package_id) DO UPDATE SET
    su_doc_class_number = EXCLUDED.su_doc_class_number,
    migrated_doc_id = EXCLUDED.migrated_doc_id,
    govinfo_collection_code = EXCLUDED.govinfo_collection_code;

-- For hearings members, we need to join through the granules
INSERT INTO bicam.hearings_members (
    hearing_id,
    bioguide_id
)
SELECT DISTINCT
    h.hearing_id,
    gm.bioguide_id
FROM
    staging_govinfo.hearings_granules hg
JOIN
    staging_govinfo.hearings_members gm
    ON hg.package_id = gm.package_id
    AND hg.granule_id = gm.granule_id
LEFT JOIN
    staging_govinfo.hearings AS h ON hg.package_id = h.package_id
WHERE
    gm.bioguide_id IS NOT NULL
AND h.package_id ~ '\d' OR h.package_id IS NULL
ON CONFLICT (hearing_id, bioguide_id) DO UPDATE SET
    hearing_id = EXCLUDED.hearing_id,
    bioguide_id = EXCLUDED.bioguide_id;

-- For members, we need to combine both sources and handle terms through granules
INSERT INTO bicam.members (
    bioguide_id,
    icpsr,
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
SELECT DISTINCT ON (COALESCE(c.bioguide_id, g.bioguide_id))
    COALESCE(c.bioguide_id, g.bioguide_id) as bioguide_id,
    i.icpsr,
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
    c.updated_at
FROM
    staging_congressional.members c
FULL OUTER JOIN
    staging_govinfo.members g ON c.bioguide_id = g.bioguide_id
LEFT JOIN _staging_congressional._members_icpsr_bioguide_crosswalk i
    ON c.bioguide_id = i.bioguide_id OR g.bioguide_id = i.bioguide_id
WHERE COALESCE(c.bioguide_id, g.bioguide_id) IS NOT NULL
ORDER BY COALESCE(c.bioguide_id, g.bioguide_id), c.updated_at DESC NULLS LAST
ON CONFLICT (bioguide_id) DO UPDATE SET
    icpsr = EXCLUDED.icpsr,
    normalized_name = EXCLUDED.normalized_name,
    direct_order_name = EXCLUDED.direct_order_name,
    inverted_order_name = EXCLUDED.inverted_order_name,
    honorific_prefix = EXCLUDED.honorific_prefix,
    first_name = EXCLUDED.first_name,
    middle_name = EXCLUDED.middle_name,
    last_name = EXCLUDED.last_name,
    suffix = EXCLUDED.suffix,
    nickname = EXCLUDED.nickname,
    party = EXCLUDED.party,
    state = EXCLUDED.state,
    district = EXCLUDED.district,
    birth_year = EXCLUDED.birth_year,
    death_year = EXCLUDED.death_year,
    official_url = EXCLUDED.official_url,
    office_address = EXCLUDED.office_address,
    office_city = EXCLUDED.office_city,
    office_district = EXCLUDED.office_district,
    office_zip = EXCLUDED.office_zip,
    office_phone = EXCLUDED.office_phone,
    sponsored_legislation_count = EXCLUDED.sponsored_legislation_count,
    cosponsored_legislation_count = EXCLUDED.cosponsored_legislation_count,
    depiction_image_url = EXCLUDED.depiction_image_url,
    depiction_attribution = EXCLUDED.depiction_attribution,
    is_current_member = EXCLUDED.is_current_member,
    updated_at = EXCLUDED.updated_at;

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
SELECT DISTINCT ON (COALESCE(c.bioguide_id, g.bioguide_id), COALESCE(c.start_year, 0), COALESCE(c.end_year, 0))
    COALESCE(c.bioguide_id, g.bioguide_id) as bioguide_id,
    c.member_type,
    c.chamber,
    COALESCE(c.congress, cd.congress) as congress,
    COALESCE(c.start_year, 0),
    COALESCE(c.end_year, 0),
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
    staging_congressional.members_terms c
FULL OUTER JOIN
    staging_govinfo.members g
    ON g.bioguide_id = c.bioguide_id
JOIN
    staging_govinfo.congressional_directories cd
    ON cd.package_id = g.package_id
WHERE COALESCE(c.bioguide_id, g.bioguide_id) IS NOT NULL
ORDER BY COALESCE(c.bioguide_id, g.bioguide_id), COALESCE(c.start_year, 0), COALESCE(c.end_year, 0)
ON CONFLICT (bioguide_id, start_year, end_year) DO UPDATE SET
    member_type = EXCLUDED.member_type,
    chamber = EXCLUDED.chamber,
    congress = EXCLUDED.congress,
    state_name = EXCLUDED.state_name,
    state_code = EXCLUDED.state_code,
    district = EXCLUDED.district,
    title = EXCLUDED.title,
    biography = EXCLUDED.biography,
    population = EXCLUDED.population,
    twitter_url = EXCLUDED.twitter_url,
    instagram_url = EXCLUDED.instagram_url,
    facebook_url = EXCLUDED.facebook_url,
    youtube_url = EXCLUDED.youtube_url,
    other_url = EXCLUDED.other_url;


-- DROP VIEW IF EXISTS bicam.missing_bioguide_ids;
-- CREATE VIEW bicam.missing_bioguide_ids AS
-- SELECT DISTINCT mt.*
-- FROM bicam.members_terms mt
-- LEFT JOIN bicam.members m ON mt.bioguide_id = m.bioguide_id
-- WHERE m.bioguide_id IS NULL;
--
-- DELETE FROM bicam.members_terms
-- WHERE bioguide_id IN (
--     SELECT bioguide_id
--     FROM bicam.missing_bioguide_ids
-- );

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
    staging_govinfo.members m
ON CONFLICT (govinfo_package_id, govinfo_granule_id, bioguide_id) DO UPDATE SET
    gpo_id = EXCLUDED.gpo_id,
    authority_id = EXCLUDED.authority_id;

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
    staging_congressional.committees
ON CONFLICT (committee_code) DO UPDATE SET
    name = EXCLUDED.name,
    chamber = EXCLUDED.chamber,
    is_subcommittee = EXCLUDED.is_subcommittee,
    is_current = EXCLUDED.is_current,
    bills_count = EXCLUDED.bills_count,
    reports_count = EXCLUDED.reports_count,
    nominations_count = EXCLUDED.nominations_count,
    updated_at = EXCLUDED.updated_at;

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
SELECT DISTINCT ON (COALESCE(c.print_id, g.print_id))
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
    staging_congressional.committeeprints c
FULL OUTER JOIN
    staging_govinfo.committeeprints g
    ON c.print_id = g.print_id
WHERE COALESCE(c.print_id, g.print_id) IS NOT NULL
ORDER BY COALESCE(c.print_id, g.print_id), c.updated_at DESC NULLS LAST, g.last_modified DESC NULLS LAST
ON CONFLICT (print_id) DO UPDATE SET
    print_jacketnumber = EXCLUDED.print_jacketnumber,
    congress = EXCLUDED.congress,
    session = EXCLUDED.session,
    chamber = EXCLUDED.chamber,
    title = EXCLUDED.title,
    pages = EXCLUDED.pages,
    print_number = EXCLUDED.print_number,
    citation = EXCLUDED.citation,
    updated_at = EXCLUDED.updated_at;

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
    staging_govinfo.committeeprints
ON CONFLICT (print_id, govinfo_package_id) DO UPDATE SET
    su_doc_class_number = EXCLUDED.su_doc_class_number,
    migrated_doc_id = EXCLUDED.migrated_doc_id,
    govinfo_collection_code = EXCLUDED.govinfo_collection_code;

INSERT INTO bicam.committeeprints_metadata_granules
    (print_id,
    govinfo_package_id,
    govinfo_granule_id)
SELECT
    p.print_id,
    g.package_id,
    g.granule_id
FROM
    staging_govinfo.committeeprints_granules g
JOIN
    staging_govinfo.committeeprints p
    ON g.package_id = p.package_id
ON CONFLICT (print_id, govinfo_package_id, govinfo_granule_id) DO UPDATE SET
    print_id = EXCLUDED.print_id,
    govinfo_package_id = EXCLUDED.govinfo_package_id,
    govinfo_granule_id = EXCLUDED.govinfo_granule_id;

-- Step 2: Insert initial data into bicam.committeereports
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
SELECT DISTINCT ON (COALESCE(c.report_id, g.report_id))
    COALESCE(c.report_id, g.report_id) as report_id,
    COALESCE(c.report_type, regexp_replace(split_part(g.report_id, '-', 1), '\d+', '')),
    COALESCE(c.report_number, regexp_replace(split_part(g.report_id, '-', 1), '\D+', '')::int),
    c.report_part,
    COALESCE(c.congress, g.congress),
    g.session,
    COALESCE(c.title, g.title),
    g.subtitle,
    COALESCE(c.chamber, LOWER(g.chamber)),
    c.citation,
    c.is_conference_report,
    COALESCE(c.issued_at, g.issued_at),
    g.pages,
    c.texts_count,
    GREATEST(c.updated_at, g.last_modified) as updated_at
FROM
    staging_congressional.committeereports c
FULL OUTER JOIN
    staging_govinfo.committeereports g
    ON c.report_id = g.report_id
WHERE COALESCE(c.report_id, g.report_id) IS NOT NULL
ORDER BY COALESCE(c.report_id, g.report_id), c.updated_at DESC NULLS LAST, g.last_modified DESC NULLS LAST
ON CONFLICT (report_id) DO UPDATE SET
    report_type = EXCLUDED.report_type,
    report_number = EXCLUDED.report_number,
    report_part = EXCLUDED.report_part,
    congress = EXCLUDED.congress,
    session = EXCLUDED.session,
    title = EXCLUDED.title,
    subtitle = EXCLUDED.subtitle,
    chamber = EXCLUDED.chamber,
    citation = EXCLUDED.citation,
    is_conference_report = EXCLUDED.is_conference_report,
    issued_at = EXCLUDED.issued_at,
    pages = EXCLUDED.pages,
    texts_count = EXCLUDED.texts_count,
    updated_at = EXCLUDED.updated_at;

-- Create a temporary view for matched_reports
DROP TABLE IF EXISTS matched_reports;
CREATE TEMP TABLE matched_reports AS
SELECT
    COALESCE(cr2.report_id, cr1.report_id) AS report_id,
    cr1.report_id AS govinfo_report_id,
    cr2.report_id AS congressional_report_id,
    cr2.report_type AS report_type,
    cr2.report_number AS report_number,
    cr2.report_part AS report_part,
    cr2.citation AS citation,
    cr2.is_conference_report AS is_conference_report,
    cr2.texts_count AS texts_count,
    cr1.pages AS pages,
    cr1.subtitle AS subtitle,
    cr1.session AS session
FROM bicam.committeereports AS cr1
JOIN bicam.committeereports AS cr2
  ON cr1.report_id = split_part(cr2.report_id, '-', 1) || '-' || split_part(cr2.report_id, '-', 3)
WHERE cr1.report_id != cr2.report_id
  AND LENGTH(cr2.report_id) - LENGTH(REPLACE(cr2.report_id, '-', '')) = 2
  AND split_part(cr2.report_id, '-', 2) ~ '^[0-9]+$'
  AND split_part(cr2.report_id, '-', 2)::int = 1;


-- Step 4: Update bicam.committeereports using the temporary view
UPDATE bicam.committeereports
SET pages = mr.pages,
    subtitle = mr.subtitle,
    session = mr.session
FROM matched_reports mr
WHERE bicam.committeereports.report_id = mr.report_id;

DELETE FROM bicam.committeereports WHERE report_id IN (
    SELECT govinfo_report_id FROM matched_reports
);

-- Insert combined data into bicam.committeereports_metadata
INSERT INTO bicam.committeereports_metadata (
    report_id,
    govinfo_package_id,
    su_doc_class_number,
    migrated_doc_id,
    govinfo_collection_code
)
SELECT DISTINCT
    COALESCE(mr.report_id, g.report_id) as report_id,
    package_id,
    su_doc_class_number,
    migrated_doc_id,
    collection_code
FROM
    staging_govinfo.committeereports g
LEFT JOIN
    matched_reports mr
    ON g.report_id = mr.govinfo_report_id
ON CONFLICT (report_id, govinfo_package_id) DO UPDATE SET
    su_doc_class_number = EXCLUDED.su_doc_class_number,
    migrated_doc_id = EXCLUDED.migrated_doc_id,
    govinfo_collection_code = EXCLUDED.govinfo_collection_code;

INSERT INTO bicam.committeereports_metadata_granules (
    report_id,
    govinfo_package_id,
    govinfo_granule_id
)
SELECT DISTINCT
    COALESCE(mr.report_id, r.report_id),
    g.package_id,
    g.granule_id
FROM
    staging_govinfo.committeereports_granules g
JOIN
    staging_govinfo.committeereports r
    ON g.package_id = r.package_id
LEFT JOIN
    matched_reports mr
    ON r.report_id = mr.govinfo_report_id
WHERE COALESCE(mr.report_id, r.report_id) IN (
    SELECT report_id FROM bicam.committeereports
)
ON CONFLICT (report_id, govinfo_package_id, govinfo_granule_id) DO UPDATE SET
    report_id = EXCLUDED.report_id,
    govinfo_package_id = EXCLUDED.govinfo_package_id,
    govinfo_granule_id = EXCLUDED.govinfo_granule_id;

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
SELECT DISTINCT ON (COALESCE(c.treaty_id, g.treaty_id))
    COALESCE(c.treaty_id, g.treaty_id) as treaty_id,
    COALESCE(c.treaty_number, regexp_replace(split_part(g.treaty_id, '-', 1), '\D+', '')::int),
    c.suffix,
    COALESCE(c.congress_received, g.congress),
    g.session as congress_received_session,
    c.congress_considered,
    c.topic,
    c.transmitted_at,
    c.in_force_at,
    c.resolution_text,
    g.summary,
    g.pages,
    c.parts_count,
    c.actions_count,
    c.old_number,
    c.old_number_display_name,
    GREATEST(c.updated_at, g.last_modified) as updated_at
FROM
    staging_congressional.treaties c
FULL OUTER JOIN
    staging_govinfo.treaties g
    ON c.treaty_id = g.treaty_id
WHERE COALESCE(c.treaty_id, g.treaty_id) IS NOT NULL
  AND COALESCE(c.treaty_id, g.treaty_id) != '-99'
ORDER BY COALESCE(c.treaty_id, g.treaty_id), c.updated_at DESC NULLS LAST, g.last_modified DESC NULLS LAST
ON CONFLICT (treaty_id) DO UPDATE SET
    treaty_number = EXCLUDED.treaty_number,
    suffix = EXCLUDED.suffix,
    congress_received = EXCLUDED.congress_received,
    congress_received_session = EXCLUDED.congress_received_session,
    congress_considered = EXCLUDED.congress_considered,
    topic = EXCLUDED.topic,
    transmitted_at = EXCLUDED.transmitted_at,
    in_force_at = EXCLUDED.in_force_at,
    resolution_text = EXCLUDED.resolution_text,
    summary = EXCLUDED.summary,
    pages = EXCLUDED.pages,
    parts_count = EXCLUDED.parts_count,
    actions_count = EXCLUDED.actions_count,
    old_number = EXCLUDED.old_number,
    old_number_display_name = EXCLUDED.old_number_display_name,
    updated_at = EXCLUDED.updated_at;

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
    staging_govinfo.treaties
ON CONFLICT (treaty_id, govinfo_package_id) DO UPDATE SET
    su_doc_class_number = EXCLUDED.su_doc_class_number,
    migrated_doc_id = EXCLUDED.migrated_doc_id,
    govinfo_collection_code = EXCLUDED.govinfo_collection_code;

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
    c.nomination_id,
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
    staging_congressional.nominations c
ON CONFLICT (nomination_id) DO UPDATE SET
    nomination_number = EXCLUDED.nomination_number,
    part_number = EXCLUDED.part_number,
    congress = EXCLUDED.congress,
    description = EXCLUDED.description,
    is_privileged = EXCLUDED.is_privileged,
    is_civilian = EXCLUDED.is_civilian,
    received_at = EXCLUDED.received_at,
    authority_date = EXCLUDED.authority_date,
    executive_calendar_number = EXCLUDED.executive_calendar_number,
    citation = EXCLUDED.citation,
    committees_count = EXCLUDED.committees_count,
    actions_count = EXCLUDED.actions_count,
    updated_at = EXCLUDED.updated_at;

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
    staging_congressional.congresses c
ON CONFLICT (congress_number) DO UPDATE SET
    name = EXCLUDED.name,
    start_year = EXCLUDED.start_year,
    end_year = EXCLUDED.end_year,
    updated_at = EXCLUDED.updated_at;

-- Congress directories
INSERT INTO bicam.congresses_directories (
    congress_number,
    title,
    issued_at,
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
SELECT DISTINCT ON (g.package_id)
    g.congress,
    g.title,
    g.issued_at,
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
    staging_govinfo.congressional_directories g
ORDER BY g.package_id, g.last_modified DESC NULLS LAST
ON CONFLICT (govinfo_package_id) DO UPDATE SET
    congress_number = EXCLUDED.congress_number,
    title = EXCLUDED.title,
    issued_at = EXCLUDED.issued_at,
    su_doc_class_number = EXCLUDED.su_doc_class_number,
    migrated_doc_id = EXCLUDED.migrated_doc_id,
    ils_system_id = EXCLUDED.ils_system_id,
    govinfo_collection_code = EXCLUDED.govinfo_collection_code,
    government_author1 = EXCLUDED.government_author1,
    government_author2 = EXCLUDED.government_author2,
    publisher = EXCLUDED.publisher,
    last_modified = EXCLUDED.last_modified;

-- Committee prints committees - combine both sources
INSERT INTO bicam.committeeprints_committees (
    print_id,
    committee_code
)
SELECT DISTINCT
    COALESCE(c.print_id, cg.print_id) as print_id,
    COALESCE(c.committee_code, g.committee_code) as committee_code
FROM
    staging_congressional.committeeprints_committees c
JOIN
    staging_govinfo.committeeprints cg
    ON c.print_id = cg.print_id
FULL OUTER JOIN
    staging_govinfo.committeeprints_committees g
    ON g.package_id = cg.package_id
WHERE COALESCE(c.print_id, cg.print_id) IS NOT NULL
AND COALESCE(c.committee_code, g.committee_code) IS NOT NULL
AND EXISTS (
    SELECT 1 FROM bicam.committees c2 
    WHERE c2.committee_code = COALESCE(c.committee_code, g.committee_code)
  )
ON CONFLICT (print_id, committee_code) DO UPDATE SET
    print_id = EXCLUDED.print_id,
    committee_code = EXCLUDED.committee_code;

-- Committee prints associated bills - combine both sources
INSERT INTO bicam.committeeprints_associated_bills (
    print_id,
    bill_id
)
SELECT DISTINCT
    COALESCE(c.print_id, cg.print_id) as print_id,
    COALESCE(c.bill_id, g.bill_id) as bill_id
FROM
    staging_congressional.committeeprints_associated_bills c
JOIN
    staging_govinfo.committeeprints cg
    ON c.print_id = cg.print_id
FULL OUTER JOIN
    staging_govinfo.committeeprints_reference_bills g
    ON cg.package_id = g.package_id
    AND c.bill_id = g.bill_id
WHERE COALESCE(c.print_id, cg.print_id) IS NOT NULL
AND COALESCE(c.bill_id, g.bill_id) IS NOT NULL
AND EXISTS (
    SELECT 1 FROM bicam.bills b 
    WHERE b.bill_id = COALESCE(c.bill_id, g.bill_id)
  )
ON CONFLICT (print_id, bill_id) DO UPDATE SET
    print_id = EXCLUDED.print_id,
    bill_id = EXCLUDED.bill_id;

-- Committee reports committees - combine both sources
INSERT INTO bicam.committees_committeereports (
    report_id,
    committee_code
)
SELECT DISTINCT
    COALESCE(mr.report_id, c.report_id, cr.report_id) as report_id,
    COALESCE(c.committee_code, g.committee_code) as committee_code
FROM
    staging_congressional.committees_committeereports c
JOIN staging_govinfo.committeereports cr
    ON c.report_id = cr.report_id
FULL OUTER JOIN
    staging_govinfo.committeereports_committees g
    ON cr.package_id = g.package_id
LEFT JOIN matched_reports mr
    ON cr.report_id = mr.govinfo_report_id
WHERE
    COALESCE(mr.report_id, c.report_id, cr.report_id) IS NOT NULL
    AND COALESCE(c.committee_code, g.committee_code) IS NOT NULL
    AND EXISTS (
        SELECT 1 FROM bicam.committees c2 
        WHERE c2.committee_code = COALESCE(c.committee_code, g.committee_code)
    )
ON CONFLICT (report_id, committee_code) DO UPDATE SET
    report_id = EXCLUDED.report_id,
    committee_code = EXCLUDED.committee_code;

-- Committee reports associated bills - combine both sources
INSERT INTO bicam.committeereports_associated_bills (
    report_id,
    bill_id
)
SELECT DISTINCT
    COALESCE(mr.report_id, c.report_id, cr.report_id) as report_id,
    COALESCE(c.bill_id, g.bill_id) as bill_id
FROM
    staging_congressional.committeereports_associated_bills c
LEFT JOIN staging_govinfo.committeereports cr
    ON c.report_id = cr.report_id
FULL OUTER JOIN
    staging_govinfo.committeereports_reference_bills g
    ON cr.package_id = g.package_id
    AND c.bill_id = g.bill_id
LEFT JOIN matched_reports mr
    ON cr.report_id = mr.govinfo_report_id
WHERE
    COALESCE(mr.report_id, c.report_id, cr.report_id) IS NOT NULL
    AND COALESCE(c.bill_id, g.bill_id) IS NOT NULL
    AND EXISTS (
        SELECT 1 FROM bicam.committeereports cr2 
        WHERE cr2.report_id = COALESCE(mr.report_id, c.report_id, cr.report_id)
    )
    AND EXISTS (
        SELECT 1 FROM bicam.bills b 
        WHERE b.bill_id = COALESCE(c.bill_id, g.bill_id)
    )
ON CONFLICT (report_id, bill_id) DO UPDATE SET
    report_id = EXCLUDED.report_id,
    bill_id = EXCLUDED.bill_id;

-- Hearings committees - combine both sources
INSERT INTO bicam.hearings_committees (
    hearing_id,
    committee_code,
    committee_name
)
WITH congressional_with_parts AS (
    SELECT
        CASE
            WHEN part_number IS NOT NULL THEN
                SPLIT_PART(hearing_id, '-', 1) || 'p' || trim_scale(part_number::numeric) || '-' || SPLIT_PART(hearing_id, '-', 2)
            ELSE hearing_id
        END as part_hearing_id,
        *
    FROM staging_congressional.hearings
)
SELECT DISTINCT
    COALESCE(hc.part_hearing_id, c.hearing_id, h.hearing_id)
    as hearing_id,
    COALESCE(c.committee_code, g.committee_code) as committee_code,
    g.committee_name
FROM
    staging_congressional.hearings_committees c
JOIN
    staging_govinfo.hearings h
    ON c.hearing_id = h.hearing_id
FULL OUTER JOIN
    staging_govinfo.hearings_committees g
    ON g.package_id = h.package_id
    AND c.committee_code = g.committee_code
LEFT JOIN
    congressional_with_parts hc
    ON c.hearing_id = hc.part_hearing_id
WHERE
    COALESCE(c.committee_code, g.committee_code) IS NOT NULL
AND h.package_id ~ '\d'
AND EXISTS (
    SELECT 1 FROM bicam.committees c2 
    WHERE c2.committee_code = COALESCE(c.committee_code, g.committee_code)
  )
ON CONFLICT (hearing_id, committee_code) DO UPDATE SET
    committee_name = EXCLUDED.committee_name;

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
    CASE WHEN action_code = '-99' THEN NULL ELSE action_code END AS action_code,
    action_date,
    text,
    action_type,
    source_system,
    source_system_code,
    calendar,
    calendar_number
FROM
    staging_congressional.bills_actions
WHERE EXISTS (
    SELECT 1 FROM bicam.bills b 
    WHERE b.bill_id = staging_congressional.bills_actions.bill_id
  )
ON CONFLICT (action_id) DO UPDATE SET
    bill_id = EXCLUDED.bill_id,
    action_code = EXCLUDED.action_code,
    action_date = EXCLUDED.action_date,
    text = EXCLUDED.text,
    action_type = EXCLUDED.action_type,
    source_system = EXCLUDED.source_system,
    source_system_code = EXCLUDED.source_system_code,
    calendar = EXCLUDED.calendar,
    calendar_number = EXCLUDED.calendar_number;

-- Bills cosponsors
INSERT INTO bicam.bills_cosponsors (
    bill_id,
    bioguide_id
)
SELECT
    bc.bill_id,
    bc.bioguide_id
FROM
    staging_congressional.bills_cosponsors bc
WHERE EXISTS (
    SELECT 1 FROM bicam.bills b 
    WHERE b.bill_id = bc.bill_id
  )
ON CONFLICT (bill_id, bioguide_id) DO UPDATE SET
    bill_id = EXCLUDED.bill_id,
    bioguide_id = EXCLUDED.bioguide_id;

-- Bills sponsors
INSERT INTO bicam.bills_sponsors (
    bill_id,
    bioguide_id
)
SELECT
    bs.bill_id,
    bs.bioguide_id
FROM
    staging_congressional.bills_sponsors bs
WHERE EXISTS (
    SELECT 1 FROM bicam.bills b 
    WHERE b.bill_id = bs.bill_id
  )
ON CONFLICT (bill_id, bioguide_id) DO UPDATE SET
    bill_id = EXCLUDED.bill_id,
    bioguide_id = EXCLUDED.bioguide_id;

-- Bills subjects
INSERT INTO bicam.bills_subjects (
    bill_id,
    subject,
    updated_at
)
SELECT
    bsub.bill_id,
    bsub.subject,
    bsub.updated_at
FROM
    staging_congressional.bills_subjects bsub
WHERE EXISTS (
    SELECT 1 FROM bicam.bills b 
    WHERE b.bill_id = bsub.bill_id
  )
ON CONFLICT (bill_id, subject) DO UPDATE SET
    updated_at = EXCLUDED.updated_at;

-- Bills summaries
INSERT INTO bicam.bills_summaries (
    bill_id,
    action_date,
    action_desc,
    text,
    version_code
)
SELECT
    bsum.bill_id,
    bsum.action_date,
    bsum.action_desc,
    bsum.text,
    bsum.version_code
FROM
    staging_congressional.bills_summaries bsum
WHERE EXISTS (
    SELECT 1 FROM bicam.bills b 
    WHERE b.bill_id = bsum.bill_id
  )
ON CONFLICT (bill_id, action_date, action_desc, version_code) DO UPDATE SET
    text = EXCLUDED.text;

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
    bt.bill_id,
    bt.title,
    bt.title_type,
    bt.bill_text_version_code,
    bt.bill_text_version_name,
    bt.chamber,
    bt.title_type_code
FROM
    staging_congressional.bills_titles bt
WHERE EXISTS (
    SELECT 1 FROM bicam.bills b 
    WHERE b.bill_id = bt.bill_id
  )
ON CONFLICT (bill_id, title, title_type) DO UPDATE SET
    bill_text_version_code = EXCLUDED.bill_text_version_code,
    bill_text_version_name = EXCLUDED.bill_text_version_name,
    chamber = EXCLUDED.chamber,
    title_type_code = EXCLUDED.title_type_code;

INSERT INTO bicam.ref_title_type_codes (title_type_code, description)
SELECT DISTINCT title_type_code::integer, title_type FROM staging_congressional.bills_titles WHERE title_type_code::integer NOT IN (select title_type_code::integer FROM bicam.ref_title_type_codes) ORDER BY title_type_code::integer
ON CONFLICT (title_type_code) DO UPDATE SET
    description = EXCLUDED.description;

-- Committee subcommittees
INSERT INTO bicam.committees_subcommittees (
    committee_code,
    subcommittee_code
)
SELECT
    committee_code,
    subcommittee_code
FROM
    staging_congressional.committees_subcommittees
WHERE EXISTS (
    SELECT 1 FROM bicam.committees c 
    WHERE c.committee_code = staging_congressional.committees_subcommittees.committee_code
  )
  AND EXISTS (
    SELECT 1 FROM bicam.committees c 
    WHERE c.committee_code = staging_congressional.committees_subcommittees.subcommittee_code
  )
ON CONFLICT (committee_code, subcommittee_code) DO UPDATE SET
    committee_code = EXCLUDED.committee_code,
    subcommittee_code = EXCLUDED.subcommittee_code;

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
    staging_congressional.members_leadership_roles
WHERE EXISTS (
    SELECT 1 FROM bicam.members m 
    WHERE m.bioguide_id = staging_congressional.members_leadership_roles.bioguide_id
  )
ON CONFLICT (bioguide_id, role, congress, chamber) DO UPDATE SET
    is_current = EXCLUDED.is_current;

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
    staging_congressional.members_party_history
WHERE EXISTS (
    SELECT 1 FROM bicam.members m 
    WHERE m.bioguide_id = staging_congressional.members_party_history.bioguide_id
  )
ON CONFLICT (bioguide_id, party_code, start_year, end_year) DO UPDATE SET
    party_name = EXCLUDED.party_name;

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
    CASE WHEN action_code = '-99' THEN NULL ELSE action_code END AS action_code,
    action_date,
    text,
    action_type
FROM
    staging_congressional.treaties_actions
WHERE EXISTS (
    SELECT 1 FROM bicam.treaties t 
    WHERE t.treaty_id = staging_congressional.treaties_actions.treaty_id
  )
ON CONFLICT (action_id) DO UPDATE SET
    treaty_id = EXCLUDED.treaty_id,
    action_code = EXCLUDED.action_code,
    action_date = EXCLUDED.action_date,
    text = EXCLUDED.text,
    action_type = EXCLUDED.action_type;

-- Treaties country parties
INSERT INTO bicam.treaties_country_parties (
    treaty_id,
    country
)
SELECT
    treaty_id,
    country
FROM
    staging_congressional.treaties_country_parties
WHERE EXISTS (
    SELECT 1 FROM bicam.treaties t 
    WHERE t.treaty_id = staging_congressional.treaties_country_parties.treaty_id
  )
ON CONFLICT (treaty_id, country) DO UPDATE SET
    treaty_id = EXCLUDED.treaty_id,
    country = EXCLUDED.country;

-- Treaties index terms
INSERT INTO bicam.treaties_index_terms (
    treaty_id,
    index_term
)
SELECT
    treaty_id,
    index_term
FROM
    staging_congressional.treaties_index_terms
WHERE EXISTS (
    SELECT 1 FROM bicam.treaties t 
    WHERE t.treaty_id = staging_congressional.treaties_index_terms.treaty_id
  )
ON CONFLICT (treaty_id, index_term) DO UPDATE SET
    treaty_id = EXCLUDED.treaty_id,
    index_term = EXCLUDED.index_term;

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
    staging_congressional.treaties_titles
WHERE EXISTS (
    SELECT 1 FROM bicam.treaties t 
    WHERE t.treaty_id = staging_congressional.treaties_titles.treaty_id
  )
ON CONFLICT (treaty_id, title, title_type) DO UPDATE SET
    treaty_id = EXCLUDED.treaty_id,
    title = EXCLUDED.title,
    title_type = EXCLUDED.title_type;

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
    na.action_id,
    na.nomination_id,
    CASE WHEN na.action_code = '-99' THEN NULL ELSE na.action_code END AS action_code,
    na.action_type,
    na.action_date,
    na.text
FROM
    staging_congressional.nominations_actions na
WHERE EXISTS (
    SELECT 1 FROM bicam.nominations n 
    WHERE n.nomination_id = na.nomination_id
  )
ON CONFLICT (action_id) DO UPDATE SET
    nomination_id = EXCLUDED.nomination_id,
    action_code = EXCLUDED.action_code,
    action_type = EXCLUDED.action_type,
    action_date = EXCLUDED.action_date,
    text = EXCLUDED.text;

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
    np.nomination_id,
    np.ordinal,
    np.position_title,
    np.organization,
    np.intro_text,
    np.nominee_count
FROM
    staging_congressional.nominations_positions np
WHERE EXISTS (
    SELECT 1 FROM bicam.nominations n 
    WHERE n.nomination_id = np.nomination_id
  )
ON CONFLICT (nomination_id, ordinal) DO UPDATE SET
    position_title = EXCLUDED.position_title,
    organization = EXCLUDED.organization,
    intro_text = EXCLUDED.intro_text,
    nominee_count = EXCLUDED.nominee_count;

-- Nominations actions committees
INSERT INTO bicam.nominations_actions_committees (
    action_id,
    nomination_id,
    committee_code
)
SELECT
    nac.action_id,
    nac.nomination_id,
    nac.committee_code
FROM
    staging_congressional.nominations_actions_committees nac
WHERE EXISTS (
    SELECT 1 FROM bicam.nominations n 
    WHERE n.nomination_id = nac.nomination_id
  )
ON CONFLICT (action_id, nomination_id, committee_code) DO UPDATE SET
    action_id = EXCLUDED.action_id,
    nomination_id = EXCLUDED.nomination_id,
    committee_code = EXCLUDED.committee_code;

-- Nominations committee activities
INSERT INTO bicam.nominations_committeeactivities (
    nomination_id,
    committee_code,
    activity_name,
    activity_date
)
SELECT
    nca.nomination_id,
    nca.committee_code,
    nca.activity_name,
    nca.activity_date
FROM
    staging_congressional.nominations_committeeactivities nca
WHERE EXISTS (
    SELECT 1 FROM bicam.nominations n 
    WHERE n.nomination_id = nca.nomination_id
  )
ON CONFLICT (nomination_id, committee_code, activity_name, activity_date) DO UPDATE SET
    nomination_id = EXCLUDED.nomination_id,
    committee_code = EXCLUDED.committee_code,
    activity_name = EXCLUDED.activity_name,
    activity_date = EXCLUDED.activity_date;

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
    nn.nomination_id,
    nn.ordinal,
    nn.first_name,
    nn.middle_name,
    nn.last_name,
    nn.prefix,
    nn.suffix,
    nn.state,
    nn.effective_date,
    nn.predecessor_name,
    nn.corps_code
FROM
    staging_congressional.nominations_nominees nn
WHERE EXISTS (
    SELECT 1 FROM bicam.nominations n 
    WHERE n.nomination_id = nn.nomination_id
  )
ON CONFLICT (nomination_id, ordinal, first_name, middle_name, last_name) DO UPDATE SET
    prefix = EXCLUDED.prefix,
    suffix = EXCLUDED.suffix,
    state = EXCLUDED.state,
    effective_date = EXCLUDED.effective_date,
    predecessor_name = EXCLUDED.predecessor_name,
    corps_code = EXCLUDED.corps_code;

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
    staging_congressional.committeemeetings
ON CONFLICT (meeting_id) DO UPDATE SET
    title = EXCLUDED.title,
    meeting_type = EXCLUDED.meeting_type,
    chamber = EXCLUDED.chamber,
    congress = EXCLUDED.congress,
    date = EXCLUDED.date,
    room = EXCLUDED.room,
    street_address = EXCLUDED.street_address,
    building = EXCLUDED.building,
    city = EXCLUDED.city,
    state = EXCLUDED.state,
    zip_code = EXCLUDED.zip_code,
    meeting_status = EXCLUDED.meeting_status,
    updated_at = EXCLUDED.updated_at;

-- Committee meetings committees
INSERT INTO bicam.committeemeetings_committees (
    meeting_id,
    committee_code
)
SELECT
    meeting_id,
    committee_code
FROM
    staging_congressional.committeemeetings_committees
WHERE EXISTS (
    SELECT 1 FROM bicam.committeemeetings cm 
    WHERE cm.meeting_id = staging_congressional.committeemeetings_committees.meeting_id
  )
  AND EXISTS (
    SELECT 1 FROM bicam.committees c 
    WHERE c.committee_code = staging_congressional.committeemeetings_committees.committee_code
  )
ON CONFLICT (meeting_id, committee_code) DO UPDATE SET
    meeting_id = EXCLUDED.meeting_id,
    committee_code = EXCLUDED.committee_code;

-- Committee meetings documents
INSERT INTO bicam.committeemeetings_meeting_documents (
    meeting_id,
    name,
    document_type,
    description,
    url
)
SELECT
    cmd.meeting_id,
    cmd.name,
    cmd.document_type,
    (scmd.payload ->> 'description') AS description,
    cmd.url
FROM
    staging_congressional.committeemeetings_meeting_documents AS cmd
LEFT JOIN bicam_raw_congressional.committeemeetings_raw AS scmd
    ON cmd.meeting_id = (payload ->> 'eventId')
ON CONFLICT (meeting_id, url) DO UPDATE SET
    name = EXCLUDED.name,
    document_type = EXCLUDED.document_type,
    description = EXCLUDED.description;

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
    staging_congressional.committeemeetings_witness_documents
WHERE EXISTS (
    SELECT 1 FROM bicam.committeemeetings cm 
    WHERE cm.meeting_id = staging_congressional.committeemeetings_witness_documents.meeting_id
  )
ON CONFLICT (meeting_id, url) DO UPDATE SET
    document_type = EXCLUDED.document_type;

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
    staging_congressional.committeemeetings_witnesses
WHERE EXISTS (
    SELECT 1 FROM bicam.committeemeetings cm 
    WHERE cm.meeting_id = staging_congressional.committeemeetings_witnesses.meeting_id
  )
ON CONFLICT (meeting_id, name, position, organization) DO UPDATE SET
    meeting_id = EXCLUDED.meeting_id,
    name = EXCLUDED.name,
    position = EXCLUDED.position,
    organization = EXCLUDED.organization;

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
    staging_congressional.bills_texts
WHERE EXISTS (
    SELECT 1 FROM bicam.bills b 
    WHERE b.bill_id = staging_congressional.bills_texts.bill_id
  )
    ON CONFLICT (bill_id, date, type) DO UPDATE SET
        raw_text = EXCLUDED.raw_text,
        formatted_text = EXCLUDED.formatted_text,
        pdf = EXCLUDED.pdf,
        xml = EXCLUDED.xml;

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
    staging_congressional.committeeprints_texts
WHERE EXISTS (
    SELECT 1 FROM bicam.committeeprints cp 
    WHERE cp.print_id = staging_congressional.committeeprints_texts.print_id
  )
ON CONFLICT (print_id, formatted_text, pdf, html, xml) DO UPDATE SET
    raw_text = EXCLUDED.raw_text,
    png = EXCLUDED.png;

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
    crt.report_id,
    crt.raw_text,
    crt.formatted_text,
    crt.formatted_text_is_errata,
    crt.pdf,
    crt.pdf_is_errata
FROM
    staging_congressional.committeereports_texts crt
WHERE crt.report_id != '-99'
  AND EXISTS (
    SELECT 1 FROM bicam.committeereports cr 
    WHERE cr.report_id = crt.report_id
  )
ON CONFLICT (report_id, formatted_text, pdf) DO UPDATE SET
    raw_text = EXCLUDED.raw_text,
    formatted_text_is_errata = EXCLUDED.formatted_text_is_errata,
    pdf_is_errata = EXCLUDED.pdf_is_errata;

-- Hearings texts - from congressional only
INSERT INTO bicam.hearings_texts (
    hearing_id,
    raw_text,
    pdf,
    formatted_text
)
WITH congressional_with_parts AS (
    SELECT
        CASE
            WHEN part_number IS NOT NULL THEN
                SPLIT_PART(hearing_id, '-', 1) || 'p' || trim_scale(part_number::numeric) || '-' || SPLIT_PART(hearing_id, '-', 2)
            ELSE hearing_id
        END as part_hearing_id,
        *
    FROM staging_congressional.hearings
)
SELECT DISTINCT ON (COALESCE(h.hearing_id, c.part_hearing_id, ht.hearing_id), pdf, formatted_text)
COALESCE(h.hearing_id, c.part_hearing_id, ht.hearing_id) as hearing_id,
    raw_text,
    pdf,
    formatted_text
FROM
    staging_congressional.hearings_texts ht
LEFT JOIN staging_govinfo.hearings_granules hg
    ON CASE WHEN position('CHRG' in SPLIT_PART(formatted_text, '/', 6)) > 0 THEN SPLIT_PART(formatted_text, '/', 6) ELSE SPLIT_PART(formatted_text, '/', 7) END = hg.granule_id OR SPLIT_PART(raw_text, '/', 6) = hg.package_id
JOIN staging_govinfo.hearings h
    ON hg.package_id = h.package_id
LEFT JOIN
    congressional_with_parts c
    ON ht.hearing_id = c.part_hearing_id
WHERE h.package_id ~ '\d'
ORDER BY COALESCE(h.hearing_id, c.part_hearing_id, ht.hearing_id), pdf, formatted_text
ON CONFLICT (hearing_id, pdf, formatted_text) DO UPDATE SET
    raw_text = EXCLUDED.raw_text;

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
    staging_congressional.bills_notes
WHERE EXISTS (
    SELECT 1 FROM bicam.bills b 
    WHERE b.bill_id = staging_congressional.bills_notes.bill_id
  )
ON CONFLICT (bill_id, note_number) DO UPDATE SET
    note_text = EXCLUDED.note_text,
    updated_at = EXCLUDED.updated_at;

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
    CURRENT_TIMESTAMP AS updated_at
FROM
    staging_congressional.bills_notes_links
WHERE EXISTS (
    SELECT 1 FROM bicam.bills b 
    WHERE b.bill_id = staging_congressional.bills_notes_links.bill_id
  )
ON CONFLICT (bill_id, note_number, link_url) DO UPDATE SET
    link_name = EXCLUDED.link_name,
    updated_at = EXCLUDED.updated_at;

-- Bills reference codes
INSERT INTO bicam.bills_reference_codes (
    bill_code_id,
    bill_id,
    reference_code
)
SELECT
    bill_code_id,
    b.bill_id,
    reference_code
FROM
    staging_govinfo.bills_reference_codes AS brc
JOIN staging_govinfo.bills AS b
    ON brc.package_id = b.package_id
ON CONFLICT (bill_code_id) DO UPDATE SET
    bill_id = EXCLUDED.bill_id,
    reference_code = EXCLUDED.reference_code;


-- Bills reference codes sections
INSERT INTO bicam.bills_reference_codes_sections (
    bill_code_id,
    code_section
)
SELECT
    bill_code_id,
    code_section
FROM
    staging_govinfo.bills_reference_codes_sections
ON CONFLICT (bill_code_id, code_section) DO UPDATE SET
    bill_code_id = EXCLUDED.bill_code_id,
    code_section = EXCLUDED.code_section;

-- Bills reference laws
INSERT INTO bicam.bills_reference_laws (
    bill_id,
    law_id,
    law_type
)
SELECT DISTINCT ON (b.bill_id, law_id)
    b.bill_id,
    law_id,
    law_type
FROM
    staging_govinfo.bills_reference_laws AS brl
JOIN staging_govinfo.bills AS b
ON brl.package_id = b.package_id
ORDER BY b.bill_id, law_id
ON CONFLICT (bill_id, law_id) DO UPDATE SET
    law_type = EXCLUDED.law_type;

-- Bills reference statutes
INSERT INTO bicam.bills_reference_statutes (
    bill_statute_id,
    bill_id,
    reference_statute
)
SELECT
    bill_statute_id,
    b.bill_id,
    reference_statute
FROM
    staging_govinfo.bills_reference_statutes AS brs
JOIN staging_govinfo.bills AS b ON brs.package_id = b.package_id
ON CONFLICT (bill_statute_id) DO UPDATE SET
    bill_id = EXCLUDED.bill_id,
    reference_statute = EXCLUDED.reference_statute;

-- Bills reference statutes pages
INSERT INTO bicam.bills_reference_statutes_pages (
    bill_statute_id,
    page
)
SELECT
    bill_statute_id,
    page
FROM
    staging_govinfo.bills_reference_statutes_pages
ON CONFLICT (bill_statute_id, page) DO UPDATE SET
    bill_statute_id = EXCLUDED.bill_statute_id,
    page = EXCLUDED.page;

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
    CURRENT_TIMESTAMP AS updated_at
FROM
    staging_congressional.committees_history
ON CONFLICT (committee_code, started_at, ended_at) DO UPDATE SET
    name = EXCLUDED.name,
    loc_name = EXCLUDED.loc_name,
    committee_type = EXCLUDED.committee_type,
    establishing_authority = EXCLUDED.establishing_authority,
    su_doc_class_number = EXCLUDED.su_doc_class_number,
    nara_id = EXCLUDED.nara_id,
    loc_linked_data_id = EXCLUDED.loc_linked_data_id,
    updated_at = EXCLUDED.updated_at;

-- Hearings dates
INSERT INTO bicam.hearings_dates (
    hearing_id,
    hearing_date
)
WITH congressional_with_parts AS (
    SELECT
        CASE
            WHEN part_number IS NOT NULL THEN
                SPLIT_PART(hearing_id, '-', 1) || 'p' || trim_scale(part_number::numeric) || '-' || SPLIT_PART(hearing_id, '-', 2)
            ELSE hearing_id
        END as part_hearing_id,
        *
    FROM staging_congressional.hearings
)
SELECT DISTINCT ON (COALESCE(h.hearing_id, c.part_hearing_id, hd.hearing_id), hearing_date)
    COALESCE(h.hearing_id, c.part_hearing_id, hd.hearing_id) AS hearing_id,
    hearing_date
FROM
    staging_congressional.hearings_dates hd
LEFT JOIN staging_congressional.hearings_texts ht
    ON hd.hearing_id = ht.hearing_id
JOIN staging_govinfo.hearings_granules hg
    ON (CASE WHEN position('CHRG' in SPLIT_PART(formatted_text, '/', 6)) > 0 THEN SPLIT_PART(formatted_text, '/', 6) ELSE SPLIT_PART(formatted_text, '/', 7) END = hg.granule_id)
    OR (CASE WHEN position('CHRG' in SPLIT_PART(formatted_text, '/', 6)) > 0 THEN SPLIT_PART(formatted_text, '/', 6) ELSE SPLIT_PART(formatted_text, '/', 7) END = hg.package_id)
JOIN staging_govinfo.hearings h
    ON hg.package_id = h.package_id
LEFT JOIN
    congressional_with_parts c
    ON hd.hearing_id = c.part_hearing_id
ORDER BY COALESCE(h.hearing_id, c.part_hearing_id, hd.hearing_id), hearing_date
ON CONFLICT (hearing_id, hearing_date) DO UPDATE SET
    hearing_id = EXCLUDED.hearing_id,
    hearing_date = EXCLUDED.hearing_date;

-- Hearings witnesses
INSERT INTO bicam.hearings_witnesses (
    hearing_id,
    witness
)
SELECT DISTINCT ON (hearing_id, witness)
    hearing_id,
    witness
FROM
    staging_govinfo.hearings_witnesses AS hw
JOIN staging_govinfo.hearings_granules AS hg
    ON hw.granule_id = hg.granule_id
JOIN staging_govinfo.hearings AS h
    ON hg.package_id = h.package_id
ORDER BY hearing_id, witness
ON CONFLICT (hearing_id, witness) DO UPDATE SET
    hearing_id = EXCLUDED.hearing_id,
    witness = EXCLUDED.witness;

-- Hearings bills
INSERT INTO bicam.hearings_bills (
    hearing_id,
    bill_id
)
SELECT DISTINCT ON (h.hearing_id, hrb.bill_id)
    h.hearing_id,
    hrb.bill_id
FROM
    staging_govinfo.hearings_reference_bills AS hrb
JOIN staging_govinfo.hearings AS h
    ON hrb.package_id = h.package_id
WHERE EXISTS (
    SELECT 1 FROM bicam.bills b 
    WHERE b.bill_id = hrb.bill_id
  )
ORDER BY h.hearing_id, hrb.bill_id
ON CONFLICT (hearing_id, bill_id) DO UPDATE SET
    hearing_id = EXCLUDED.hearing_id,
    bill_id = EXCLUDED.bill_id;

-- Committee meetings associated bills
INSERT INTO bicam.committeemeetings_associated_bills (
    meeting_id,
    bill_id
)
SELECT
    meeting_id,
    bill_id
FROM
    staging_congressional.committeemeetings_associated_bills
WHERE EXISTS (
    SELECT 1 FROM bicam.bills b 
    WHERE b.bill_id = staging_congressional.committeemeetings_associated_bills.bill_id
  )
ON CONFLICT (meeting_id, bill_id) DO UPDATE SET
    meeting_id = EXCLUDED.meeting_id,
    bill_id = EXCLUDED.bill_id;

-- Committee meetings associated treaties
INSERT INTO bicam.committeemeetings_associated_treaties (
    meeting_id,
    treaty_id
)
SELECT
    meeting_id,
    treaty_id
FROM
    staging_congressional.committeemeetings_associated_treaties
WHERE EXISTS (
    SELECT 1 FROM bicam.treaties t 
    WHERE t.treaty_id = staging_congressional.committeemeetings_associated_treaties.treaty_id
  )
ON CONFLICT (meeting_id, treaty_id) DO UPDATE SET
    meeting_id = EXCLUDED.meeting_id,
    treaty_id = EXCLUDED.treaty_id;

-- Committee meetings associated nominations
INSERT INTO bicam.committeemeetings_associated_nominations (
    meeting_id,
    nomination_id
)
SELECT
    cman.meeting_id,
    cman.nomination_id
FROM
    staging_congressional.committeemeetings_associated_nominations cman
WHERE EXISTS (
    SELECT 1 FROM bicam.nominations n 
    WHERE n.nomination_id = cman.nomination_id
  )
ON CONFLICT (meeting_id, nomination_id) DO UPDATE SET
    meeting_id = EXCLUDED.meeting_id,
    nomination_id = EXCLUDED.nomination_id;

-- Committee meetings associated hearings
INSERT INTO bicam.committeemeetings_associated_hearings (
    meeting_id,
    hearing_id
)
WITH congressional_with_parts AS (
    SELECT
        CASE
            WHEN part_number IS NOT NULL THEN
                SPLIT_PART(hearing_id, '-', 1) || 'p' || trim_scale(part_number::numeric) || '-' || SPLIT_PART(hearing_id, '-', 2)
            ELSE hearing_id
        END as part_hearing_id,
        *
    FROM staging_congressional.hearings
)
SELECT DISTINCT ON (meeting_id, COALESCE(h.hearing_id, c.part_hearing_id, cah.hearing_id))
    meeting_id,
    COALESCE(h.hearing_id, c.part_hearing_id, cah.hearing_id)
        as hearing_id
FROM
    staging_congressional.committeemeetings_associated_hearings cah
LEFT JOIN staging_congressional.hearings_texts ht
    ON cah.hearing_id = ht.hearing_id
JOIN staging_govinfo.hearings_granules hg
    ON CASE WHEN position('CHRG' in SPLIT_PART(formatted_text, '/', 6)) > 0 THEN SPLIT_PART(formatted_text, '/', 6) ELSE SPLIT_PART(formatted_text, '/', 7) END = hg.granule_id OR SPLIT_PART(raw_text, '/', 6) = hg.package_id
JOIN staging_govinfo.hearings h
    ON hg.package_id = h.package_id
LEFT JOIN
    congressional_with_parts c
    ON cah.hearing_id = c.part_hearing_id
ORDER BY meeting_id, COALESCE(h.hearing_id, c.part_hearing_id, cah.hearing_id)
ON CONFLICT (meeting_id, hearing_id) DO UPDATE SET
    meeting_id = EXCLUDED.meeting_id,
    hearing_id = EXCLUDED.hearing_id;

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
    staging_congressional.bills_cbocostestimates
WHERE EXISTS (
    SELECT 1 FROM bicam.bills b 
    WHERE b.bill_id = staging_congressional.bills_cbocostestimates.bill_id
  )
ON CONFLICT (bill_id, pub_date, url) DO UPDATE SET
    description = EXCLUDED.description,
    title = EXCLUDED.title;

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
    staging_congressional.bills_related_bills
WHERE EXISTS (
    SELECT 1 FROM bicam.bills b 
    WHERE b.bill_id = staging_congressional.bills_related_bills.bill_id
  )
ON CONFLICT (bill_id, related_bill_id) DO UPDATE SET
    identification_entity = EXCLUDED.identification_entity;

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
    staging_congressional.bills_actions_recorded_votes
WHERE EXISTS (
    SELECT 1 FROM bicam.bills b 
    WHERE b.bill_id = staging_congressional.bills_actions_recorded_votes.bill_id
  )
ON CONFLICT (action_id, bill_id, url) DO UPDATE SET
    chamber = EXCLUDED.chamber,
    congress = EXCLUDED.congress,
    date = EXCLUDED.date,
    roll_number = EXCLUDED.roll_number,
    session = EXCLUDED.session;

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
    staging_congressional.congresses_sessions
ON CONFLICT (congress_number, session, chamber, type) DO UPDATE SET
    start_date = EXCLUDED.start_date,
    end_date = EXCLUDED.end_date;

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
    staging_govinfo.congressional_directories cd
JOIN
    staging_govinfo.congressional_directories_isbn i
    ON cd.package_id = i.package_id
WHERE isbn IS NOT NULL AND isbn != ''
ON CONFLICT (congress_number, govinfo_package_id, isbn) DO UPDATE SET
    congress_number = EXCLUDED.congress_number,
    govinfo_package_id = EXCLUDED.govinfo_package_id,
    isbn = EXCLUDED.isbn;

-- Nominations associated hearings
INSERT INTO bicam.nominations_associated_hearings (
    nomination_id,
    hearing_id
)
WITH congressional_with_parts AS (
    SELECT
        CASE
            WHEN part_number IS NOT NULL THEN
                SPLIT_PART(hearing_id, '-', 1) || 'p' || trim_scale(part_number::numeric) || '-' || SPLIT_PART(hearing_id, '-', 2)
            ELSE hearing_id
        END as part_hearing_id,
        *
    FROM staging_congressional.hearings
)
SELECT DISTINCT ON (nomination_id, COALESCE(h.hearing_id, c.part_hearing_id, nah.hearing_id))
    nomination_id,
        COALESCE(h.hearing_id, c.part_hearing_id, nah.hearing_id)
        as hearing_id
FROM
    staging_congressional.nominations_associated_hearings nah
LEFT JOIN staging_congressional.hearings_texts ht
    ON nah.hearing_id = ht.hearing_id
JOIN staging_govinfo.hearings_granules hg
    ON CASE WHEN position('CHRG' in SPLIT_PART(formatted_text, '/', 6)) > 0 THEN SPLIT_PART(formatted_text, '/', 6) ELSE SPLIT_PART(formatted_text, '/', 7) END = hg.granule_id OR SPLIT_PART(raw_text, '/', 6) = hg.package_id
JOIN staging_govinfo.hearings h
    ON hg.package_id = h.package_id
LEFT JOIN
    congressional_with_parts c
    ON nah.hearing_id = c.part_hearing_id
ORDER BY nomination_id, COALESCE(h.hearing_id, c.part_hearing_id, nah.hearing_id)
ON CONFLICT (nomination_id, hearing_id) DO UPDATE SET
    nomination_id = EXCLUDED.nomination_id,
    hearing_id = EXCLUDED.hearing_id;

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
    staging_govinfo.treaties t
JOIN
    staging_govinfo.treaties_granules g
    ON t.package_id = g.package_id
ON CONFLICT (treaty_id, govinfo_package_id, govinfo_granule_id) DO UPDATE SET
    treaty_id = EXCLUDED.treaty_id,
    govinfo_package_id = EXCLUDED.govinfo_package_id,
    govinfo_granule_id = EXCLUDED.govinfo_granule_id;

-- Hearings metadata granules
INSERT INTO bicam.hearings_metadata_granules (
    hearing_id,
    govinfo_package_id,
    govinfo_granule_id
)
WITH congressional_with_parts AS (
    SELECT
        CASE
            WHEN part_number IS NOT NULL THEN
                SPLIT_PART(hearing_id, '-', 1) || 'p' || trim_scale(part_number::numeric) || '-' || SPLIT_PART(hearing_id, '-', 2)
            ELSE hearing_id
        END as part_hearing_id,
        *
    FROM staging_congressional.hearings
)
SELECT DISTINCT
    COALESCE(c.part_hearing_id, h.hearing_id)
    AS hearing_id,
    h.package_id,
    g.granule_id
FROM
    staging_govinfo.hearings h
JOIN
    staging_govinfo.hearings_granules g
    ON h.package_id = g.package_id
LEFT JOIN congressional_with_parts c
    ON h.hearing_id = c.part_hearing_id
WHERE EXISTS (
    SELECT 1 FROM bicam.hearings_metadata hm 
    WHERE hm.hearing_id = COALESCE(c.part_hearing_id, h.hearing_id)
      AND hm.govinfo_package_id = h.package_id
  )
ON CONFLICT (hearing_id, govinfo_package_id, govinfo_granule_id) DO UPDATE SET
    hearing_id = EXCLUDED.hearing_id,
    govinfo_package_id = EXCLUDED.govinfo_package_id,
    govinfo_granule_id = EXCLUDED.govinfo_granule_id;

-- Add missing committees bills
INSERT INTO bicam.committees_bills (
    committee_code,
    bill_id,
    relationship_type,
    committee_action_date,
    updated_at
)
SELECT DISTINCT
    committee_code,
    bill_id,
    relationship_type,
    committee_action_date,
    updated_at
FROM
    staging_congressional.committees_bills
WHERE EXISTS (
    SELECT 1 FROM bicam.bills b 
    WHERE b.bill_id = staging_congressional.committees_bills.bill_id
  )
ON CONFLICT (committee_code, bill_id, relationship_type, committee_action_date) DO UPDATE SET
    updated_at = EXCLUDED.updated_at;

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
    staging_congressional.bills_laws
WHERE EXISTS (
    SELECT 1 FROM bicam.bills b 
    WHERE b.bill_id = staging_congressional.bills_laws.bill_id
  )
ON CONFLICT (bill_id, law_id) DO UPDATE SET
    law_number = EXCLUDED.law_number,
    law_type = EXCLUDED.law_type;

-- Add missing committee reports members
INSERT INTO bicam.committeereports_members (
    report_id,
    bioguide_id
)
SELECT DISTINCT ON (COALESCE(mr.report_id, cr.report_id), bioguide_id)
    COALESCE(mr.report_id, cr.report_id),
    bioguide_id
FROM
    staging_govinfo.committeereports_members AS crm
JOIN staging_govinfo.committeereports AS cr
    ON crm.package_id = cr.package_id
LEFT JOIN matched_reports mr
    ON cr.report_id = mr.govinfo_report_id
ORDER BY COALESCE(mr.report_id, cr.report_id), bioguide_id
ON CONFLICT (report_id, bioguide_id) DO UPDATE SET
    report_id = EXCLUDED.report_id,
    bioguide_id = EXCLUDED.bioguide_id;

-- Add missing committee reports associated treaties
INSERT INTO bicam.committeereports_associated_treaties (
    report_id,
    treaty_id
)
SELECT
    crat.report_id,
    crat.treaty_id
FROM
    staging_congressional.committeereports_associated_treaties crat
WHERE EXISTS (
    SELECT 1 FROM bicam.committeereports cr 
    WHERE cr.report_id = crat.report_id
  )
  AND EXISTS (
    SELECT 1 FROM bicam.treaties t 
    WHERE t.treaty_id = crat.treaty_id
  )
ON CONFLICT (report_id, treaty_id) DO UPDATE SET
    report_id = EXCLUDED.report_id,
    treaty_id = EXCLUDED.treaty_id;

INSERT INTO bicam.amendments_amended_bills (
    amendment_id,
    bill_id
)
SELECT
    amendment_id,
    bill_id
FROM staging_congressional.amendments_amended_bills
WHERE EXISTS (
    SELECT 1 FROM bicam.bills b 
    WHERE b.bill_id = staging_congressional.amendments_amended_bills.bill_id
  )
ON CONFLICT (amendment_id, bill_id) DO UPDATE SET
    amendment_id = EXCLUDED.amendment_id,
    bill_id = EXCLUDED.bill_id;

INSERT INTO bicam.amendments_amended_treaties (
    amendment_id,
    treaty_id
)
SELECT
    amendment_id,
    treaty_id
FROM staging_congressional.amendments_amended_treaties
WHERE EXISTS (
    SELECT 1 FROM bicam.treaties t 
    WHERE t.treaty_id = staging_congressional.amendments_amended_treaties.treaty_id
  )
ON CONFLICT (amendment_id, treaty_id) DO UPDATE SET
    amendment_id = EXCLUDED.amendment_id,
    treaty_id = EXCLUDED.treaty_id;

INSERT INTO bicam.amendments_cosponsors (
    amendment_id,
    bioguide_id
)
SELECT
    amendment_id,
    bioguide_id
FROM staging_congressional.amendments_cosponsors
WHERE EXISTS (
    SELECT 1 FROM bicam.amendments a 
    WHERE a.amendment_id = staging_congressional.amendments_cosponsors.amendment_id
  )
  AND EXISTS (
    SELECT 1 FROM bicam.members m 
    WHERE m.bioguide_id = staging_congressional.amendments_cosponsors.bioguide_id
  )
ON CONFLICT (amendment_id, bioguide_id) DO UPDATE SET
    amendment_id = EXCLUDED.amendment_id,
    bioguide_id = EXCLUDED.bioguide_id;

INSERT INTO bicam.amendments_sponsors (
    amendment_id,
    bioguide_id
)
SELECT
    amendment_id,
    bioguide_id
FROM staging_congressional.amendments_sponsors
WHERE EXISTS (
    SELECT 1 FROM bicam.amendments a 
    WHERE a.amendment_id = staging_congressional.amendments_sponsors.amendment_id
  )
  AND EXISTS (
    SELECT 1 FROM bicam.members m 
    WHERE m.bioguide_id = staging_congressional.amendments_sponsors.bioguide_id
  )
ON CONFLICT (amendment_id, bioguide_id) DO UPDATE SET
    amendment_id = EXCLUDED.amendment_id,
    bioguide_id = EXCLUDED.bioguide_id;

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
    staging_congressional.bills_actions_committees
WHERE EXISTS (
    SELECT 1 FROM bicam.bills b 
    WHERE b.bill_id = staging_congressional.bills_actions_committees.bill_id
  )
ON CONFLICT (action_id, bill_id, committee_code) DO UPDATE SET
    action_id = EXCLUDED.action_id,
    bill_id = EXCLUDED.bill_id,
    committee_code = EXCLUDED.committee_code;

INSERT INTO bicam.crosswalk_bills_voteview (
    bill_id,
    voteview_bill_id,
    congress
)
SELECT DISTINCT ON (bill_id, voteview_bill_id) bill_id, UPPER(bill_type) || bill_number AS voteview_bill_id, congress FROM bicam.bills
WHERE bill_type != 'cen_doc_h'
ON CONFLICT (bill_id, voteview_bill_id) DO UPDATE SET
    voteview_bill_id = EXCLUDED.voteview_bill_id,
    congress = EXCLUDED.congress;


-- UPDATE bicam_join_test.bills SET amendments_count = (SELECT COUNT(*) FROM bicam_join_test.amendments_amended_bills WHERE bill_id = bicam_join_test.bills.bill_id),
--                        texts_count = (SELECT COUNT(*) FROM bicam_join_test.bills_texts WHERE bill_id = bicam_join_test.bills.bill_id),
--                        actions_count = (SELECT COUNT(*) FROM bicam_join_test.bills_actions WHERE bill_id = bicam_join_test.bills.bill_id),
--                        cosponsors_count = (SELECT COUNT(*) FROM bicam_join_test.bills_cosponsors WHERE bill_id = bicam_join_test.bills.bill_id),
--                        subjects_count = (SELECT COUNT(*) FROM bicam_join_test.bills_subjects WHERE bill_id = bicam_join_test.bills.bill_id),
--                        summaries_count = (SELECT COUNT(*) FROM bicam_join_test.bills_summaries WHERE bill_id = bicam_join_test.bills.bill_id),
--                         titles_count = (SELECT COUNT(*) FROM bicam_join_test.bills_titles WHERE bill_id = bicam_join_test.bills.bill_id);
--
-- UPDATE bicam_join_test.amendments SET cosponsors_count = (SELECT COUNT(*) FROM bicam_join_test.amendments_cosponsors WHERE amendment_id = bicam_join_test.amendments.amendment_id),
--                             actions_count = (SELECT COUNT(*) FROM bicam_join_test.amendments_actions WHERE amendment_id = bicam_join_test.amendments.amendment_id),
--                             amendments_to_amendment_count = (SELECT COUNT(*) FROM bicam_join_test.amendments_amended_amendments WHERE amendment_id = bicam_join_test.amendments.amendment_id),;
--
-- UPDATE bicam_join_test.committeereports SET texts_count = (SELECT COUNT(*) FROM bicam_join_test.committeereports_texts WHERE report_id = bicam_join_test.committeereports.report_id);
--
-- UPDATE bicam_join_test.committees SET bills_count = (SELECT COUNT(*) FROM bicam_join_test.committees_bills WHERE committee_code = bicam_join_test.committees.committee_code),
--                            reports_count = (SELECT COUNT(*) FROM bicam_join_test.committees_committeereports WHERE committee_code = bicam_join_test.committees.committee_code),
--                            nominations_count = (SELECT COUNT(DISTINCT committee_code) FROM bicam_join_test.nominations_committeeactivities WHERE committee_code = bicam_join_test.committees.committee_code);
--
-- UPDATE bicam_join_test.members SET sponsored_legislation_count = (SELECT COUNT(*) FROM bicam_join_test.bills_sponsors WHERE bioguide_id = bicam_join_test.members.bioguide_id),
--                         cosponsored_legislation_count = (SELECT COUNT(*) FROM bicam_join_test.bills_cosponsors WHERE bioguide_id = bicam_join_test.members.bioguide_id);
--
--
-- UPDATE bicam_join_test.nominations SET actions_count = (SELECT COUNT(*) FROM bicam_join_test.nominations_actions WHERE nomination_id = bicam_join_test.nominations.nomination_id),
--                             committees_count = (SELECT COUNT(DISTINCT committee_code) FROM bicam_join_test.nominations_committeeactivities WHERE nomination_id = bicam_join_test.nominations.nomination_id);

        -- If we get here, everything succeeded
        RAISE NOTICE 'Data join completed successfully';

    EXCEPTION
        WHEN OTHERS THEN
            -- Rollback the transaction on any error
            RAISE NOTICE 'Error occurred: %', SQLERRM;
            RAISE NOTICE 'Rolling back all changes...';
            RAISE;
    END;
END $$;


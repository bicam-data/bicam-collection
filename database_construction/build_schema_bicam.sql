DROP SCHEMA IF EXISTS bicam CASCADE;
CREATE SCHEMA IF NOT EXISTS bicam;

BEGIN;

SET CONSTRAINTS ALL DEFERRED;

-- 1. Foundational tables
CREATE TABLE IF NOT EXISTS bicam.congresses(
    congress_number INTEGER PRIMARY KEY,
    name TEXT,
    start_year INTEGER,
    end_year INTEGER,
    updated_at TIMESTAMP WITH TIME ZONE
);

CREATE TABLE IF NOT EXISTS bicam.congresses_sessions(
    congress_number INTEGER,
    session INTEGER,
    chamber TEXT,
    type TEXT,
    start_date DATE,
    end_date DATE,
    FOREIGN KEY (congress_number) REFERENCES bicam.congresses (congress_number)
        DEFERRABLE INITIALLY DEFERRED,
    UNIQUE (congress_number, session, chamber, type)
);

CREATE TABLE IF NOT EXISTS bicam.congresses_directories(
    congress_number INTEGER,
    title TEXT,
    issued_at DATE,
    txt_url TEXT,
    pdf_url TEXT,
    govinfo_package_id TEXT,
    su_doc_class_number TEXT,
    migrated_doc_id TEXT,
    ils_system_id TEXT,
    govinfo_collection_code TEXT,
    government_author1 TEXT,
    government_author2 TEXT,
    publisher TEXT,
    last_modified TIMESTAMP WITH TIME ZONE,
    FOREIGN KEY (congress_number) REFERENCES bicam.congresses(congress_number)
        DEFERRABLE INITIALLY DEFERRED,
    UNIQUE (congress_number, govinfo_package_id)
);

CREATE TABLE IF NOT EXISTS bicam.congresses_directories_isbn(
    congress_number INTEGER,
    govinfo_package_id TEXT,
    isbn TEXT,
    FOREIGN KEY (congress_number, govinfo_package_id) REFERENCES bicam.congresses_directories(congress_number, govinfo_package_id)
        DEFERRABLE INITIALLY DEFERRED,
    UNIQUE (congress_number, govinfo_package_id, isbn)
);

-- Reference tables with no dependencies
CREATE TABLE bicam.ref_bill_summary_version_codes AS
SELECT
    version_code::integer as version_code,
    actionDesc::text as action_description,
    chamber
FROM (
    VALUES
        ('00', 'Introduced in House', 'house'),
        ('00', 'Introduced in Senate', 'senate'),
        ('01', 'Reported to Senate with amendment(s)', 'senate'),
        ('02', 'Reported to Senate amended, 1st committee reporting', 'senate'),
        ('03', 'Reported to Senate amended, 2nd committee reporting', 'senate'),
        ('04', 'Reported to Senate amended, 3rd committee reporting', 'senate'),
        ('07', 'Reported to House', 'house'),
        ('08', 'Reported to House, Part I', 'house'),
        ('09', 'Reported to House, Part II', 'house'),
        ('12', 'Reported to Senate without amendment, 1st committee reporting', 'senate'),
        ('13', 'Reported to Senate without amendment, 2nd committee reporting', 'senate'),
        ('17', 'Reported to House with amendment(s)', 'house'),
        ('18', 'Reported to House amended, Part I', 'house'),
        ('19', 'Reported to House amended Part II', 'house'),
        ('20', 'Reported to House amended, Part III', 'house'),
        ('21', 'Reported to House amended, Part IV', 'house'),
        ('22', 'Reported to House amended, Part V', 'house'),
        ('25', 'Reported to Senate', 'senate'),
        ('28', 'Reported to House without amendment, Part I', 'house'),
        ('29', 'Reported to House without amendment, Part II', 'house'),
        ('31', 'Reported to House without amendment, Part IV', 'house'),
        ('33', 'Laid on table in House', 'house'),
        ('34', 'Indefinitely postponed in Senate', 'senate'),
        ('35', 'Passed Senate amended', 'senate'),
        ('36', 'Passed House amended', 'house'),
        ('37', 'Failed of passage in Senate', 'senate'),
        ('38', 'Failed of passage in House', 'house'),
        ('39', 'Senate agreed to House amendment with amendment', 'senate'),
        ('40', 'House agreed to Senate amendment with amendment', 'house'),
        ('43', 'Senate disagreed to House amendment', 'senate'),
        ('44', 'House disagreed to Senate amendment', 'house'),
        ('45', 'Senate receded and concurred with amendment', 'senate'),
        ('46', 'House receded and concurred with amendment', 'house'),
        ('47', 'Conference report filed in Senate', 'senate'),
        ('48', 'Conference report filed in House', 'house'),
        ('49', 'Public Law', NULL),
        ('51', 'Line item veto by President', NULL),
        ('52', 'Passed Senate amended, 2nd occurrence', 'senate'),
        ('53', 'Passed House', 'house'),
        ('54', 'Passed House, 2nd occurrence', 'house'),
        ('55', 'Passed Senate', 'senate'),
        ('56', 'Senate vitiated passage of bill after amendment', 'senate'),
        ('58', 'Motion to recommit bill as amended by Senate', 'senate'),
        ('59', 'House agreed to Senate amendment', 'house'),
        ('60', 'Senate agreed to House amendment with amendment, 2nd occurrence', 'senate'),
        ('62', 'House agreed to Senate amendment with amendment, 2nd occurrence', 'house'),
        ('66', 'House receded and concurred with amendment, 2nd occurrence', 'house'),
        ('70', 'House agreed to Senate amendment without amendment', 'house'),
        ('71', 'Senate agreed to House amendment without amendment', 'senate'),
        ('74', 'Senate agreed to House amendment', 'senate'),
        ('77', 'Discharged from House committee', 'house'),
        ('78', 'Discharged from Senate committee', 'senate'),
        ('79', 'Reported to House without amendment', 'house'),
        ('80', 'Reported to Senate without amendment', 'senate'),
        ('81', 'Passed House without amendment', 'house'),
        ('82', 'Passed Senate without amendment', 'senate'),
        ('83', 'Conference report filed in Senate, 2nd conference report', 'senate'),
        ('86', 'Conference report filed in House, 2nd conference report', 'house'),
        ('87', 'Conference report filed in House, 3rd conference report', 'house')
) AS v (version_code, actionDesc, chamber);

ALTER TABLE bicam.ref_bill_summary_version_codes
ADD CONSTRAINT unique_version_code_chamber UNIQUE (version_code, chamber);

CREATE TABLE bicam.ref_title_type_codes AS
SELECT
    titleTypeCode::INTEGER as title_type_code,
    description::TEXT as description
FROM (
    VALUES
        (6, 'Official Title as Introduced'),
        (7, 'Official Titles as Amended by House'),
        (8, 'Official Titles as Amended by Senate'),
        (9, 'Official Title as Agreed to by House and Senate'),
        (14, 'Short Titles as Introduced'),
        (17, 'Short Titles as Passed House'),
        (18, 'Short Titles as Passed Senate'),
        (19, 'Short Titles as Enacted'),
        (22, 'Short Titles as Introduced for portions of this bill'),
        (23, 'Short Titles as Reported to House for portions of this bill'),
        (24, 'Short Titles as Reported to Senate for portions of this bill'),
        (25, 'Short Titles as Passed House for portions of this bill'),
        (26, 'Short Titles as Passed Senate for portions of this bill'),
        (27, 'Short Titles as Enacted for portions of this bill'),
        (30, 'Popular Title'),
        (45, 'Display Title'),
        (101, 'Short Title(s) as Introduced'),
        (102, 'Short Title(s) as Reported to House'),
        (103, 'Short Title(s) as Reported to Senate'),
        (104, 'Short Title(s) as Passed House'),
        (105, 'Short Title(s) as Passed Senate'),
        (106, 'Short Title(s) as Introduced for portions of this bill'),
        (107, 'Short Title(s) as Reported to House for portions of this bill'),
        (108, 'Short Title(s) as Reported to Senate for portions of this bill'),
        (109, 'Short Title(s) as Passed House for portions of this bill'),
        (110, 'Short Title(s) as Passed Senate for portions of this bill'),
        (147, 'Short Title(s) from ENR (Enrolled) bill text'),
        (250, 'Short Title(s) from Engrossed Amendment Senate'),
        (253, 'Short Title(s) from Engrossed Amendment House for portions of this bill'),
        (254, 'Short Title(s) from Engrossed Amendment Senate for portions of this bill')
) AS v (titleTypeCode, description);

ALTER TABLE bicam.ref_title_type_codes
ADD PRIMARY KEY (title_type_code);


CREATE TABLE bicam.ref_bill_version_codes (
    version_code TEXT PRIMARY KEY,
    version_name  TEXT,
    description TEXT NOT NULL,
    chamber TEXT
);

-- Insert data
INSERT INTO bicam.ref_bill_version_codes (version_name, version_code, description, chamber) VALUES
('Amendment (Senate)', 'AS', 'An alternate name for this version is Senate Amendment Ordered to be Printed. This version contains an amendment that has been ordered to be printed.', 'Senate'),
('Additional Sponsors (House)', 'ASH', 'An alternate name for this version is House Sponsors or Cosponsors Added or Withdrawn. This version is used to add or delete cosponsor names. When used, it most often shows numerous cosponsors being added.', 'House'),
('Agreed to (House)', 'ATH', 'An alternate name for this version is Agreed to by House. This version is a simple or concurrent resolution as agreed to in the House of Representatives.', 'House'),
('Agreed to (Senate)', 'ATS', 'An alternate name for this version is Agreed to by Senate. This version is a simple or concurrent resolution as agreed to in the Senate.', 'Senate'),
('Committee Discharged (House)', 'CDH', 'An alternate name for this version is House Committee Discharged from Further Consideration. This version is a bill or resolution as it was when the committee to which the bill or resolution has been referred has been discharged from its consideration to make it available for floor consideration.', 'House,Senate'),
('Committee Discharged (Senate)', 'CDS', 'An alternate name for this version is Senate Committee Discharged from Further Consideration. This version is a bill or resolution as it was when the committee to which the bill or resolution has been referred has been discharged from its consideration to make it available for floor consideration.', 'Senate,House'),
('Considered and Passed (House)', 'CPH', 'Considered and Passed House – An alternate name for this version is Considered and Passed by House. This version is a bill or joint resolution as considered and passed.', 'House'),
('Considered and Passed (Senate)', 'CPS', 'An alternate name for this version is Considered and Passed by Senate. This version is a bill or joint resolution as considered and passed.', 'Senate,House'),
('Engrossed Amendment (House)', 'EAH', 'An alternate name for this version is Engrossed Amendment as Agreed to by House. This version is the official copy of a bill or joint resolution as passed, including the text as amended by floor action, and certified by the Clerk of the House before it is sent to the Senate. Often this is the engrossment of an amendment in the nature of a substitute, an amendment which replaces the entire text of a measure. It strikes out everything after the enacting or resolving clause and inserts a version which may be somewhat, substantially, or entirely different.', 'House,Senate'),
('Engrossed Amendment (Senate)', 'EAS', 'An alternate name for this version is Engrossed Amendment as Agreed to by Senate. This version is the official copy of the amendment to a bill or joint resolution as passed, including the text as amended by floor action, and certified by the Secretary of the Senate before it is sent to the House. Often this is the engrossment of an amendment in the nature of a substitute, an amendment which replaces the entire text of a measure. It strikes out everything after the enacting or resolving clause and inserts a version which may be somewhat, substantially, or entirely different.', 'House,Senate'),
('Engrossed (House)', 'EH', 'An alternate name for this version is Engrossed as Agreed to or Passed by House. This version is the official copy of the bill or joint resolution as passed, including the text as amended by floor action, and certified by the Clerk of the House before it is sent to the Senate.', 'House'),
('Engrossed and Deemed Passed by House', 'EPH', 'This version is the official copy of the bill or joint resolution as passed and certified by the Clerk of the House before it is sent to the Senate. See H. J. RES. 280 from the 101st Congress for an example of this bill version.', 'House'),
('Enrolled', 'ENR', 'An alternate name for this version is Enrolled as Agreed to or Passed by Both House and Senate. This version is the final official copy of the bill or joint resolution which both the House and the Senate have passed in identical form. After it is certified by the chief officer of the house in which it originated (the Clerk of the House or the Secretary of the Senate), then signed by the House Speaker and the Senate President Pro Tempore, the measure is sent to the President for signature.', 'Joint,Senate,House'),
('Engrossed (Senate)', 'ES', 'An alternate name for this version is Engrossed as Agreed to or Passed by Senate. This version is the official copy of the bill or joint resolution as passed, including the text as amended by floor action, and certified by the Secretary of the Senate before it is sent to the House.', 'Senate'),
('Failed Amendment (House)', 'FAH', 'This amendment has failed in the House.', 'House'),
('Failed Passage (House)', 'FPH', 'Bill or resolution that failed to pass the House.', 'House'),
('Failed Passage (Senate)', 'FPS', 'Bill or resolution that failed to pass the Senate.', 'Senate'),
('Held at Desk (House)', 'HDH', 'An alternate name for this bill version is Ordered Held at House Desk after being Received from Senate. This version has been held at the desk in the House.', 'House'),
('Held at Desk (Senate)', 'HDS', 'An alternate name for this bill version is Ordered Held at Senate Desk after being Received from House. This version is a bill or resolution as received in the Senate from the House which has been ordered to be held at the desk, sometimes in preparation for going to conference. It is available to be called up for consideration by unanimous consent.', 'Senate'),
('Introduced (House)', 'IH', 'This version is a bill or resolution as formally presented by a member of Congress to a clerk when the House is in session.', 'House'),
('Indefinitely Postponed (House)', 'IPH', 'This version is a bill or resolution as it was when consideration was suspended with no date specified for continuing its consideration.', 'House'),
('Indefinitely Postponed (Senate)', 'IPS', 'This version is a bill or resolution as it was when consideration was suspended with no date specified for continuing its consideration.', 'Senate,House'),
('Introduced (Senate)', 'IS', 'This version is a bill or resolution as formally presented by a member of Congress to a clerk when the Senate is in session.', 'Senate'),
('Laid on Table (House)', 'LTH', 'This version is a bill or resolution as laid on the table which disposes of it immediately, finally, and adversely via a motion without a direct vote on its substance.', 'House,Senate'),
('Laid on Table (Senate)', 'LTS', 'This version was laid on the table in the Senate. See also Laid on Table in House.', 'Senate,House'),
('Ordered to be Printed (House)', 'OPH', 'This version was ordered to be printed by the House. See also Ordered to be Printed Senate.', 'House'),
('Ordered to be Printed (Senate)', 'OPS', 'This version was ordered to be printed by the Senate. For example, in the 105th Congress S. 1173 was considered at length by the Senate, returned to the Senate calendar, ordered to be printed. Then its text was inserted into its companion House bill which was passed by the Senate.', 'Senate'),
('Previous Action Vitiated', 'PAV', 'This version is a bill or resolution as it was when an action previously taken on it was undone or invalidated. For example in the 102nd Congress for H.R. 2321 the Senate action discharging the Energy Committee and amending and passing the bill was vitiated by unanimous consent. The bill was amended, reported, and passed anew.', 'Senate,House'),
('Placed on Calendar (House)', 'PCH', 'This version is a bill or resolution as placed on one of the five House calendars. It is eligible for floor consideration, but a place on a calendar does not guarantee consideration.', 'House,Senate'),
('Placed on Calendar (Senate)', 'PCS', 'This version is a bill or resolution as placed on one of the two Senate calendars. It is eligible for floor consideration, but a place on a calendar does not guarantee consideration.', 'Senate,House'),
('Public Print', 'PP', 'Any bill from the House or Senate may be issued as a public print. If a bill is issued as a Public Print more copies will be printed than are printed for an engrossed version. Public prints also number the amendments made by the last chamber to pass it. Public Prints are typically published by the Senate to show Senate amendments to House bills. They typically contain the text of a House bill, indicating portions struck, plus Senate amendments in italics. They are routinely ordered for appropriations bills, but the Senate occasionally by unanimous consent orders public prints of other significant bills.', 'Senate,House'),
('Printed as Passed', 'PAP', 'This version is a public print of a bill as passed. Generally, appropriation bills receive a PP designation while non-appropriation bills receive a PAP designation. See also Public Print.', 'Senate,House'),
('Ordered to be Printed with House Amendment', 'PWAH', 'This version shows Senate amendments to a House bill. It is similar to a Public Print from the Senate, except that it does not include portions struck, only the Senate amendment in the nature of a substitute in italics. See S. 1059 from the 106th Congress for an example of this bill version on a Senate bill.', 'House,Senate'),
('Referred with Amendments (House)', 'RAH', 'This version was referred with amendments to the House.', 'House'),
('Referred with Amendments (Senate)', 'RAS', 'This version was referred with amendments to the Senate.', 'Senate'),
('Reference Change (House)', 'RCH', 'An alternate name for this bill version is Referred to Different or Additional House Committee. This version is a bill or resolution as re-referred to a different or additional House committee. It may have been discharged from the committee to which it was originally referred then referred to a different committee, referred to an additional committee sequentially, or reported by the original committee then referred to an additional committee. See S. 1016 for an example of this bill version on a Senate bill.', 'House,Senate'),
('Reference Change (Senate)', 'RCS', 'An alternate name for this version is Referred to Different or Additional Senate Committee. This version is a bill or resolution as it was re-referred to a different or additional Senate committee. It may have been discharged from the committee to which it was originally referred then referred to a different committee, referred to an additional committee sequentially, or reported by the original committee then referred to an additional committee. See H.R. 1502 from the 105th Congress for an example of this bill version on a House bill.', 'Senate,House'),
('Received in (House)', 'RDH', 'An alternate name for this bill version is Received in House from Senate. This version is a bill or resolution as passed or agreed to in the Senate which has been sent to and received in the House. See the 105th Congress for an example of this bill version.', 'House'),
('Received in (Senate)', 'RDS', 'An alternate name for this bill version is Received in Senate from House. This version is a bill or resolution as it was passed or agreed to in the House which has been sent to and received in the Senate.', 'Senate'),
('Re-engrossed Amendment (House)', 'REAH', 'This version is a re-engrossed amendment in the House.', 'House,Senate'),
('Re-engrossed Amendment (Senate)', 'RES', 'This version is a re-engrossed amendment in the Senate. See also Engrossed Amendment Senate.', 'Senate,House'),
('Re-enrolled Bill', 'RENR', 'This version has been re-enrolled.', 'Joint,House,Senate'),
('Referred in (House)', 'RFH', 'An alternate name for this bill version is Referred to House Committee after being Received from Senate. This version is a bill or resolution as passed or agreed to in the Senate which has been sent to, received in the House, and referred to House committee or committees.', 'Senate,House'),
('Referred in (Senate)', 'RFS', 'An alternate name for this bill version is Referred to Senate Committee after being Received from House. This version is a bill or resolution as passed or agreed to in the House which has been sent to, received in the Senate, and referred to Senate committee or committees.', 'House,Senate'),
('Reported in (House)', 'RH', 'This version is a bill or resolution as reported by the committee or one of the committees to which it was referred, including changes, if any, made in committee. The bill or resolution is usually accompanied by a committee report which describes the measure, the committee''s views on it, its costs, and the changes it proposes to make in existing law. The bill or resolution is then available for floor consideration. This version occurs to both House and Senate bills.', 'House,Senate'),
('Returned to House by Unanimous Consent', 'RHUC', 'A bill that was returned to the House by Unanimous Consent within the Senate', 'House,Senate'),
('Referral Instructions (House)', 'RIH', 'An alternate name for this bill version is Referred to House Committee with Instructions. This version is a bill or resolution as referred or re-referred to committee with instructions to take some action on it. Invariably in the House the instructions require the committee to report the measure forthwith with specified amendments.', 'House,Senate'),
('Referral Instructions (Senate)', 'RIS', 'An alternate name for this bill version is Referred to Senate Committee with Instructions. This version is a bill or resolution as referred or re-referred to committee with instructions to take some action on it. Often in the Senate the instructions require the committee to report the measure forth with specified amendments.', 'Senate,House'),
('Reported in (Senate)', 'RS', 'This version is a bill or resolution as reported by the committee or one of the committees to which it was referred, including changes, if any, made in committee. The bill or resolution is usually accompanied by a committee report which describes the measure, the committee''s views on it, its costs, and the changes it proposes to make in existing law. The bill or resolution is then available for floor consideration.', 'Senate,House'),
('Referred to Committee (House)', 'RTH', 'Bill or resolution as referred or re-referred to a House committee or committees. See 104th Congress for an example of this bill version.', 'House'),
('Referred to Committee (Senate)', 'RTS', 'Bill or resolution as referred or re-referred to a Senate committee or committees.', 'Senate'),
('Additional Sponsors (Senate)', 'SAS', 'Additional sponsors have been added to this version.', 'Senate'),
('Sponsor Change', 'SC', 'This version is used to change sponsors.', 'House');

-- 2. Base tables that have no or simple dependencies
CREATE TABLE IF NOT EXISTS bicam.members( -- coalesce the members that don't exist from govinfo, e.g. delegates and residentcommissioners
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

CREATE TABLE IF NOT EXISTS bicam.members_metadata(
    bioguide_id TEXT,
    govinfo_granule_id TEXT,
    govinfo_package_id TEXT,
    gpo_id TEXT,
    authority_id TEXT,
    FOREIGN KEY (govinfo_package_id) REFERENCES bicam.congresses_directories(govinfo_package_id)
    INITIALLY DEFERRED,
    FOREIGN KEY (bioguide_id) REFERENCES bicam.members(bioguide_id),
    UNIQUE (govinfo_package_id, govinfo_granule_id, bioguide_id)
);

CREATE TABLE IF NOT EXISTS bicam.committees(
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
CREATE TABLE IF NOT EXISTS bicam.bills( -- coalesce congressional with govinfo to get version, current chamber, the new bools, pages
    bill_id TEXT PRIMARY KEY,
    bill_type TEXT,
    bill_number TEXT,
    congress INTEGER,
    title TEXT,
    origin_chamber TEXT,
    current_chamber TEXT,
    version_code TEXT,
    introduced_at DATE,
    constitutional_authority_statement TEXT,
    is_law BOOLEAN,
    is_appropriation BOOLEAN,
    is_private BOOLEAN,
    policy_area TEXT,
    pages INTEGER,
    actions_count INTEGER,
    amendments_count INTEGER,
    committees_count INTEGER,
    cosponsors_count INTEGER,
    summaries_count INTEGER,
    subjects_count INTEGER,
    titles_count INTEGER,
    texts_count INTEGER,
    updated_at TIMESTAMP WITH TIME ZONE,
    FOREIGN KEY (congress) REFERENCES bicam.congresses (congress_number)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (version_code) REFERENCES bicam.ref_bill_version_codes(version_code)
);


CREATE TABLE IF NOT EXISTS bicam.bills_metadata(
    bill_id TEXT,
    govinfo_package_id TEXT,
    su_doc_class_number TEXT,
    migrated_doc_id TEXT,
    stock_number TEXT,
    child_ils_system_id TEXT,
    parent_ils_system_id TEXT,
    govinfo_collection_code TEXT,
    FOREIGN KEY (bill_id) REFERENCES bicam.bills(bill_id),
    UNIQUE (bill_id, govinfo_package_id)
);

CREATE TABLE IF NOT EXISTS bicam.hearings(
    hearing_id TEXT PRIMARY KEY,
    hearing_jacketnumber TEXT,
    loc_id TEXT,
    title TEXT,
    congress INTEGER,
    session INTEGER,
    chamber TEXT,
    is_appropriation BOOLEAN,
    hearing_number INTEGER,
    part_number TEXT,
    citation TEXT,
    pages INTEGER,
    updated_at TIMESTAMP WITH TIME ZONE,
    FOREIGN KEY (congress) REFERENCES bicam.congresses (congress_number)
        DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE IF NOT EXISTS bicam.hearings_metadata(
    hearing_id TEXT,
    govinfo_package_id TEXT,
    su_doc_class_number TEXT,
    migrated_doc_id TEXT,
    govinfo_collection_code TEXT,
    FOREIGN KEY (hearing_id) REFERENCES bicam.hearings (hearing_id)
        DEFERRABLE INITIALLY DEFERRED,
    UNIQUE (hearing_id, govinfo_package_id)
);

CREATE TABLE IF NOT EXISTS bicam.hearings_metadata_granules(
    hearing_id TEXT,
    govinfo_package_id TEXT,
    govinfo_granule_id TEXT,
    FOREIGN KEY (hearing_id, govinfo_package_id) REFERENCES bicam.hearings_metadata (hearing_id, govinfo_package_id)
        DEFERRABLE INITIALLY DEFERRED,
    UNIQUE (hearing_id, govinfo_package_id, govinfo_granule_id)
);


CREATE TABLE IF NOT EXISTS bicam.treaties(
    treaty_id TEXT PRIMARY KEY,
    treaty_number INTEGER,
    suffix TEXT,
    congress_received INTEGER,
    congress_received_session INTEGER,
    congress_considered INTEGER,
    topic TEXT,
    transmitted_at TIMESTAMP WITH TIME ZONE,
    in_force_at TIMESTAMP WITH TIME ZONE,
    resolution_text TEXT,
    summary TEXT,
    pages INTEGER,
    parts_count INTEGER,
    actions_count INTEGER,
    old_number TEXT,
    old_number_display_name TEXT,
    updated_at TIMESTAMP WITH TIME ZONE,
    FOREIGN KEY (congress_received) REFERENCES bicam.congresses (congress_number)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (congress_considered) REFERENCES bicam.congresses (congress_number)
        DEFERRABLE INITIALLY DEFERRED
);


CREATE TABLE IF NOT EXISTS bicam.treaties_metadata(
    treaty_id TEXT,
    govinfo_package_id TEXT,
    su_doc_class_number TEXT,
    migrated_doc_id TEXT,
    govinfo_collection_code TEXT,
    FOREIGN KEY (treaty_id) REFERENCES bicam.treaties (treaty_id)
        DEFERRABLE INITIALLY DEFERRED,
    UNIQUE (treaty_id, govinfo_package_id)
);

CREATE TABLE IF NOT EXISTS bicam.treaties_metadata_granules(
    treaty_id TEXT,
    govinfo_package_id TEXT,
    govinfo_granule_id TEXT,
    FOREIGN KEY (treaty_id, govinfo_package_id) REFERENCES bicam.treaties_metadata (treaty_id, govinfo_package_id)
        DEFERRABLE INITIALLY DEFERRED,
    UNIQUE (treaty_id, govinfo_package_id, govinfo_granule_id)
);

CREATE TABLE IF NOT EXISTS bicam.nominations(
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
    FOREIGN KEY (congress) REFERENCES bicam.congresses (congress_number)
        DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE IF NOT EXISTS bicam.amendments(
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
    FOREIGN KEY (congress) REFERENCES bicam.congresses (congress_number)
        DEFERRABLE INITIALLY DEFERRED
);

-- Dependent on bills and version codes:
CREATE TABLE IF NOT EXISTS bicam.bills_summaries(
    bill_id TEXT,
    action_date DATE,
    action_desc TEXT,
    text TEXT,
    version_code INTEGER,
    FOREIGN KEY (bill_id) REFERENCES bicam.bills (bill_id)
        DEFERRABLE INITIALLY DEFERRED,
    UNIQUE (bill_id, action_date, action_desc, version_code)
);

-- Dependent on bills and title type codes:
CREATE TABLE IF NOT EXISTS bicam.bills_titles( -- check this with bills_short_titles
    bill_id TEXT,
    title TEXT,
    title_type TEXT,
    bill_text_version_code TEXT,
    bill_text_version_name TEXT,
    chamber TEXT,
    title_type_code INTEGER,
    FOREIGN KEY (bill_id) REFERENCES bicam.bills (bill_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (title_type_code) REFERENCES bicam.ref_title_type_codes (title_type_code)
        DEFERRABLE INITIALLY DEFERRED,
    PRIMARY KEY (bill_id, title, title_type)
);

-- Depends only on congresses
CREATE TABLE IF NOT EXISTS bicam.committeeprints(
    print_id TEXT PRIMARY KEY,
    print_jacketnumber TEXT,
    congress INTEGER,
    session INTEGER,
    chamber TEXT,
    title TEXT,
    pages INTEGER,
    print_number TEXT,
    citation TEXT,
    updated_at TIMESTAMP,
    FOREIGN KEY (congress) REFERENCES bicam.congresses (congress_number)
        DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE IF NOT EXISTS bicam.committeeprints_metadata(
    print_id TEXT,
    govinfo_package_id TEXT,
    su_doc_class_number TEXT,
    migrated_doc_id TEXT,
    govinfo_collection_code TEXT,
    FOREIGN KEY (print_id) REFERENCES bicam.committeeprints (print_id)
        DEFERRABLE INITIALLY DEFERRED,
    UNIQUE (print_id, govinfo_package_id)
);

CREATE TABLE IF NOT EXISTS bicam.committeeprints_metadata_granules(
    print_id TEXT,
    govinfo_package_id TEXT,
    govinfo_granule_id TEXT,
    FOREIGN KEY (print_id, govinfo_package_id) REFERENCES bicam.committeeprints_metadata (print_id, govinfo_package_id)
        DEFERRABLE INITIALLY DEFERRED,
    UNIQUE (print_id, govinfo_package_id, govinfo_granule_id)
);


-- No direct FKs, but will link later
CREATE TABLE IF NOT EXISTS bicam.committeereports(
    report_id TEXT PRIMARY KEY,
    report_type TEXT,
    report_number INTEGER,
    report_part INTEGER,
    congress INTEGER,
    session INTEGER,
    title TEXT,
    subtitle TEXT,
    chamber TEXT,
    citation TEXT,
    is_conference_report BOOLEAN,
    issued_at TIMESTAMP WITH TIME ZONE,
    pages INTEGER,
    texts_count INTEGER,
    updated_at TIMESTAMP WITH TIME ZONE
);

CREATE TABLE IF NOT EXISTS bicam.committeereports_metadata(
    report_id TEXT,
    govinfo_package_id TEXT,
    su_doc_class_number TEXT,
    migrated_doc_id TEXT,
    govinfo_collection_code TEXT,
    FOREIGN KEY (report_id) REFERENCES bicam.committeereports (report_id)
        DEFERRABLE INITIALLY DEFERRED,
    UNIQUE (report_id, govinfo_package_id)
);

CREATE TABLE IF NOT EXISTS bicam.committeereports_metadata_granules(
    report_id TEXT,
    govinfo_package_id TEXT,
    govinfo_granule_id TEXT,
    FOREIGN KEY (report_id, govinfo_package_id) REFERENCES bicam.committeereports_metadata (report_id, govinfo_package_id)
        DEFERRABLE INITIALLY DEFERRED,
    UNIQUE (report_id, govinfo_package_id, govinfo_granule_id)
);

-- Depends on congresses and must link to bills, treaties, nominations, hearings
CREATE TABLE IF NOT EXISTS bicam.committeemeetings(
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
    FOREIGN KEY (congress) REFERENCES bicam.congresses (congress_number)
        DEFERRABLE INITIALLY DEFERRED
);

-- Now create the amendments dependent tables
CREATE TABLE IF NOT EXISTS bicam.amendments_sponsors(
    amendment_id TEXT,
    bioguide_id TEXT,
    FOREIGN KEY (amendment_id) REFERENCES bicam.amendments (amendment_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (bioguide_id) REFERENCES bicam.members (bioguide_id)
        DEFERRABLE INITIALLY DEFERRED,
    PRIMARY KEY (amendment_id, bioguide_id)
);

CREATE TABLE IF NOT EXISTS bicam.amendments_cosponsors(
    amendment_id TEXT,
    bioguide_id TEXT,
    FOREIGN KEY (amendment_id) REFERENCES bicam.amendments (amendment_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (bioguide_id) REFERENCES bicam.members (bioguide_id)
        DEFERRABLE INITIALLY DEFERRED,
    PRIMARY KEY (amendment_id, bioguide_id)
);

CREATE TABLE IF NOT EXISTS bicam.amendments_amended_bills(
    amendment_id TEXT,
    bill_id TEXT,
    FOREIGN KEY (amendment_id) REFERENCES bicam.amendments (amendment_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (bill_id) REFERENCES bicam.bills (bill_id)
        DEFERRABLE INITIALLY DEFERRED,
    PRIMARY KEY (amendment_id, bill_id)
);

CREATE TABLE IF NOT EXISTS bicam.amendments_amended_treaties(
    amendment_id TEXT,
    treaty_id TEXT,
    FOREIGN KEY (amendment_id) REFERENCES bicam.amendments (amendment_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (treaty_id) REFERENCES bicam.treaties (treaty_id)
        DEFERRABLE INITIALLY DEFERRED,
    PRIMARY KEY (amendment_id, treaty_id)
);

CREATE TABLE IF NOT EXISTS bicam.amendments_amended_amendments(
    amendment_id TEXT,
    amended_amendment_id TEXT,
    FOREIGN KEY (amendment_id) REFERENCES bicam.amendments (amendment_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (amended_amendment_id) REFERENCES bicam.amendments (amendment_id)
        DEFERRABLE INITIALLY DEFERRED,
    PRIMARY KEY (amendment_id, amended_amendment_id)
);

CREATE TABLE IF NOT EXISTS bicam.amendments_actions(
    action_id TEXT PRIMARY KEY,
    amendment_id TEXT,
    action_code TEXT,
    action_date TIMESTAMP WITH TIME ZONE,
    text TEXT,
    action_type TEXT,
    source_system TEXT,
    source_system_code INTEGER,
    FOREIGN KEY (amendment_id) REFERENCES bicam.amendments (amendment_id)
        DEFERRABLE INITIALLY DEFERRED,
    UNIQUE (action_id, amendment_id)
);

CREATE TABLE IF NOT EXISTS bicam.amendments_actions_recorded_votes(
    action_id TEXT,
    amendment_id TEXT,
    chamber TEXT,
    congress INTEGER,
    date TIMESTAMP WITH TIME ZONE,
    roll_number INTEGER,
    session INTEGER,
    url TEXT,
    FOREIGN KEY (action_id, amendment_id) REFERENCES bicam.amendments_actions (action_id, amendment_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (congress) REFERENCES bicam.congresses (congress_number)
        DEFERRABLE INITIALLY DEFERRED,
    UNIQUE (action_id, amendment_id, url)
);

CREATE TABLE IF NOT EXISTS bicam.amendments_texts(
    amendment_id TEXT,
    date TIMESTAMP WITH TIME ZONE,
    type TEXT,
    raw_text TEXT,
    pdf TEXT,
    html TEXT,
    FOREIGN KEY (amendment_id) REFERENCES bicam.amendments (amendment_id)
        DEFERRABLE INITIALLY DEFERRED,
    PRIMARY KEY (amendment_id, date, type)
);

-- Bills related tables
CREATE TABLE IF NOT EXISTS bicam.bills_texts(
    bill_id TEXT,
    date TEXT,
    type TEXT,
    raw_text TEXT,
    formatted_text TEXT,
    pdf TEXT,
    xml TEXT,
    FOREIGN KEY (bill_id) REFERENCES bicam.bills (bill_id)
        DEFERRABLE INITIALLY DEFERRED,
    UNIQUE (bill_id, date, type)
);

CREATE TABLE IF NOT EXISTS bicam.bills_actions(
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
    FOREIGN KEY (bill_id) REFERENCES bicam.bills (bill_id)
        DEFERRABLE INITIALLY DEFERRED,
    UNIQUE (action_id, bill_id)
);

CREATE TABLE IF NOT EXISTS bicam.bills_actions_committees(
    action_id TEXT,
    bill_id TEXT,
    committee_code TEXT,
    FOREIGN KEY (action_id, bill_id) REFERENCES bicam.bills_actions (action_id, bill_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (committee_code) REFERENCES bicam.committees (committee_code)
        DEFERRABLE INITIALLY DEFERRED,
    UNIQUE (action_id, bill_id, committee_code)
);

CREATE TABLE IF NOT EXISTS bicam.bills_actions_recorded_votes(
    action_id TEXT,
    bill_id TEXT,
    chamber TEXT,
    congress INTEGER,
    date TIMESTAMP WITH TIME ZONE,
    roll_number INTEGER,
    session INTEGER,
    url TEXT,
    FOREIGN KEY (action_id, bill_id) REFERENCES bicam.bills_actions (action_id, bill_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (congress) REFERENCES bicam.congresses (congress_number)
        DEFERRABLE INITIALLY DEFERRED,
    UNIQUE (action_id, bill_id, url)
);

CREATE TABLE IF NOT EXISTS bicam.bills_related_bills(
    bill_id TEXT,
    related_bill_id TEXT,
    identification_entity TEXT,
    FOREIGN KEY (bill_id) REFERENCES bicam.bills (bill_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (related_bill_id) REFERENCES bicam.bills (bill_id)
        DEFERRABLE INITIALLY DEFERRED,
    PRIMARY KEY (bill_id, related_bill_id)
);

CREATE TABLE IF NOT EXISTS bicam.bills_cbocostestimates(
    bill_id TEXT,
    description TEXT,
    pub_date TIMESTAMP WITH TIME ZONE,
    title TEXT,
    url TEXT,
    FOREIGN KEY (bill_id) REFERENCES bicam.bills (bill_id)
        DEFERRABLE INITIALLY DEFERRED,
    PRIMARY KEY (bill_id, pub_date, url)
);

CREATE TABLE IF NOT EXISTS bicam.bills_cosponsors(
    bill_id TEXT,
    bioguide_id TEXT,
    FOREIGN KEY (bill_id) REFERENCES bicam.bills (bill_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (bioguide_id) REFERENCES bicam.members (bioguide_id)
        DEFERRABLE INITIALLY DEFERRED,
    PRIMARY KEY (bill_id, bioguide_id)
);

CREATE TABLE IF NOT EXISTS bicam.bills_laws(
    bill_id TEXT,
    law_id TEXT,
    law_number TEXT,
    law_type TEXT,
    FOREIGN KEY (bill_id) REFERENCES bicam.bills (bill_id)
        DEFERRABLE INITIALLY DEFERRED,
    PRIMARY KEY (bill_id, law_id)
);

CREATE TABLE IF NOT EXISTS bicam.bills_notes(
    bill_id TEXT,
    note_number INTEGER,
    note_text TEXT,
    updated_at TIMESTAMP WITH TIME ZONE,
    FOREIGN KEY (bill_id) REFERENCES bicam.bills (bill_id)
        DEFERRABLE INITIALLY DEFERRED,
    PRIMARY KEY (bill_id, note_number)
);

CREATE TABLE IF NOT EXISTS bicam.bills_notes_links(
    bill_id TEXT,
    note_number INTEGER,
    link_name TEXT,
    link_url TEXT,
    updated_at TIMESTAMP WITH TIME ZONE,
    FOREIGN KEY (bill_id, note_number) REFERENCES bicam.bills_notes (bill_id, note_number)
        DEFERRABLE INITIALLY DEFERRED,
    UNIQUE (bill_id, note_number, link_url)
);

CREATE TABLE IF NOT EXISTS bicam.bills_sponsors(
    bill_id TEXT,
    bioguide_id TEXT,
    FOREIGN KEY (bill_id) REFERENCES bicam.bills (bill_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (bioguide_id) REFERENCES bicam.members (bioguide_id)
        DEFERRABLE INITIALLY DEFERRED,
    PRIMARY KEY (bill_id, bioguide_id)
);

CREATE TABLE IF NOT EXISTS bicam.bills_subjects(
    bill_id TEXT,
    subject TEXT,
    updated_at TIMESTAMP WITH TIME ZONE,
    FOREIGN KEY (bill_id) REFERENCES bicam.bills (bill_id)
        DEFERRABLE INITIALLY DEFERRED,
    PRIMARY KEY (bill_id, subject)
);


CREATE TABLE IF NOT EXISTS bicam.bills_reference_codes(
    bill_code_id TEXT PRIMARY KEY,
    bill_id TEXT,
    reference_code TEXT, -- combination of label and title
    FOREIGN KEY (bill_id) REFERENCES bicam.bills(bill_id)
    INITIALLY DEFERRED
);

CREATE TABLE IF NOT EXISTS bicam.bills_reference_codes_sections(
    bill_code_id TEXT,
    code_section TEXT,
    FOREIGN KEY (bill_code_id) REFERENCES bicam.bills_reference_codes(bill_code_id)
    INITIALLY DEFERRED,
    UNIQUE (bill_code_id, code_section)
);

CREATE TABLE IF NOT EXISTS bicam.bills_reference_laws(
    bill_id TEXT,
    law_id TEXT, -- fix law_id
    law_type TEXT,
    FOREIGN KEY (bill_id) REFERENCES bicam.bills(bill_id)
    INITIALLY DEFERRED,
    PRIMARY KEY (bill_id, law_id)
);

CREATE TABLE IF NOT EXISTS bicam.bills_reference_statutes(
    bill_statute_id TEXT PRIMARY KEY,
    bill_id TEXT,
    reference_statute TEXT, -- combine label and title
    FOREIGN KEY (bill_id) REFERENCES bicam.bills(bill_id)
    INITIALLY DEFERRED
);

CREATE TABLE IF NOT EXISTS bicam.bills_reference_statutes_pages(
    bill_statute_id TEXT,
    page TEXT, -- combine label and title
    FOREIGN KEY (bill_statute_id) REFERENCES bicam.bills_reference_statutes(bill_statute_id)
    INITIALLY DEFERRED,
    UNIQUE (bill_statute_id, page)
);


-- committeeprints related
CREATE TABLE IF NOT EXISTS bicam.committeeprints_associated_bills(
    print_id TEXT,
    bill_id TEXT,
    FOREIGN KEY (print_id) REFERENCES bicam.committeeprints (print_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (bill_id) REFERENCES bicam.bills (bill_id)
        DEFERRABLE INITIALLY DEFERRED,
    PRIMARY KEY (print_id, bill_id)
);

CREATE TABLE IF NOT EXISTS bicam.committeeprints_committees(
    print_id TEXT,
    committee_code TEXT,
    FOREIGN KEY (print_id) REFERENCES bicam.committeeprints (print_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (committee_code) REFERENCES bicam.committees (committee_code)
        DEFERRABLE INITIALLY DEFERRED,
    PRIMARY KEY (print_id, committee_code)
);

CREATE TABLE IF NOT EXISTS bicam.committeeprints_texts(
    print_id TEXT,
    raw_text TEXT,
    formatted_text TEXT,
    pdf TEXT,
    html TEXT,
    xml TEXT,
    png TEXT,
    FOREIGN KEY (print_id) REFERENCES bicam.committeeprints (print_id)
        DEFERRABLE INITIALLY DEFERRED,
    UNIQUE (print_id, formatted_text, pdf, html, xml)
);

-- committeereports related
CREATE TABLE IF NOT EXISTS bicam.committeereports_associated_bills(
    report_id TEXT,
    bill_id TEXT,
    FOREIGN KEY (report_id) REFERENCES bicam.committeereports (report_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (bill_id) REFERENCES bicam.bills (bill_id)
        DEFERRABLE INITIALLY DEFERRED,
    PRIMARY KEY (report_id, bill_id)
);

CREATE TABLE IF NOT EXISTS bicam.committeereports_associated_treaties(
    report_id TEXT,
    treaty_id TEXT,
    FOREIGN KEY (report_id) REFERENCES bicam.committeereports (report_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (treaty_id) REFERENCES bicam.treaties (treaty_id)
        DEFERRABLE INITIALLY DEFERRED,
    PRIMARY KEY (report_id, treaty_id)
);

CREATE TABLE IF NOT EXISTS bicam.committeereports_texts(
    report_id TEXT,
    raw_text TEXT,
    formatted_text TEXT,
    formatted_text_is_errata BOOLEAN,
    pdf TEXT,
    pdf_is_errata BOOLEAN,
    FOREIGN KEY (report_id) REFERENCES bicam.committeereports (report_id)
        DEFERRABLE INITIALLY DEFERRED,
    UNIQUE (report_id, formatted_text, pdf)
);

CREATE TABLE IF NOT EXISTS bicam.committeereports_members(
    report_id TEXT,
    bioguide_id TEXT,
    FOREIGN KEY (report_id) REFERENCES bicam.committeereports (report_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (bioguide_id) REFERENCES bicam.members (bioguide_id)
        DEFERRABLE INITIALLY DEFERRED,
    UNIQUE (report_id, bioguide_id)
);

-- committees related
CREATE TABLE IF NOT EXISTS bicam.committees_bills(
    committee_code TEXT,
    bill_id TEXT,
    relationship_type TEXT,
    committee_action_date TIMESTAMP WITH TIME ZONE,
    updated_at TIMESTAMP WITH TIME ZONE,
    FOREIGN KEY (committee_code) REFERENCES bicam.committees (committee_code)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (bill_id) REFERENCES bicam.bills (bill_id)
        DEFERRABLE INITIALLY DEFERRED,
    UNIQUE (committee_code, bill_id, relationship_type, committee_action_date)
);

CREATE TABLE IF NOT EXISTS bicam.committees_history(
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
    FOREIGN KEY (committee_code) REFERENCES bicam.committees (committee_code)
        DEFERRABLE INITIALLY DEFERRED,
    UNIQUE (committee_code, started_at, ended_at)
);

CREATE TABLE IF NOT EXISTS bicam.committees_subcommittees(
    committee_code TEXT,
    subcommittee_code TEXT,
    FOREIGN KEY (committee_code) REFERENCES bicam.committees (committee_code)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (subcommittee_code) REFERENCES bicam.committees (committee_code)
        DEFERRABLE INITIALLY DEFERRED,
    PRIMARY KEY (committee_code, subcommittee_code)
);

CREATE TABLE IF NOT EXISTS bicam.committees_committeereports(
    committee_code TEXT,
    report_id TEXT,
    FOREIGN KEY (committee_code) REFERENCES bicam.committees (committee_code)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (report_id) REFERENCES bicam.committeereports (report_id)
        DEFERRABLE INITIALLY DEFERRED,
    PRIMARY KEY (committee_code, report_id)
);

-- hearings related
CREATE TABLE IF NOT EXISTS bicam.hearings_committees(
    hearing_id TEXT,
    committee_code TEXT,
    committee_name TEXT,
    FOREIGN KEY (hearing_id) REFERENCES bicam.hearings (hearing_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (committee_code) REFERENCES bicam.committees (committee_code)
        DEFERRABLE INITIALLY DEFERRED,
    UNIQUE (hearing_id, committee_code)
);

CREATE TABLE IF NOT EXISTS bicam.hearings_dates(
    hearing_id TEXT,
    hearing_date DATE,
    FOREIGN KEY (hearing_id) REFERENCES bicam.hearings (hearing_id)
        DEFERRABLE INITIALLY DEFERRED,
    PRIMARY KEY (hearing_id, hearing_date)
);

CREATE TABLE IF NOT EXISTS bicam.hearings_texts(
    hearing_id TEXT,
    raw_text TEXT,
    pdf TEXT,
    formatted_text TEXT,
    FOREIGN KEY (hearing_id) REFERENCES bicam.hearings (hearing_id)
        DEFERRABLE INITIALLY DEFERRED,
    UNIQUE (hearing_id, pdf, formatted_text)
);

CREATE TABLE IF NOT EXISTS bicam.hearings_members( -- found via linking hearing_id in the hearings_metadata_granules table
    hearing_id TEXT,
    bioguide_id TEXT,
    FOREIGN KEY (hearing_id) REFERENCES bicam.hearings (hearing_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (bioguide_id) REFERENCES bicam.members (bioguide_id)
        DEFERRABLE INITIALLY DEFERRED,
    UNIQUE (hearing_id, bioguide_id)
);

CREATE TABLE IF NOT EXISTS bicam.hearings_witnesses(
    hearing_id TEXT,
    witness TEXT,
    FOREIGN KEY (hearing_id) REFERENCES bicam.hearings (hearing_id)
        DEFERRABLE INITIALLY DEFERRED,
    UNIQUE (hearing_id, witness)
);

CREATE TABLE IF NOT EXISTS bicam.hearings_bills(
    hearing_id TEXT,
    bill_id TEXT,
    FOREIGN KEY (hearing_id) REFERENCES bicam.hearings (hearing_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (bill_id) REFERENCES bicam.bills (bill_id)
        DEFERRABLE INITIALLY DEFERRED,
    UNIQUE (hearing_id, bill_id)

);

-- members related
CREATE TABLE IF NOT EXISTS bicam.members_terms(
    bioguide_id TEXT,
    member_type TEXT,
    chamber TEXT,
    congress INTEGER,
    start_year INTEGER,
    end_year INTEGER,
    state_name TEXT,
    state_code TEXT,
    district INTEGER,
    title TEXT,   -- add members data via congress connected to congresses_directories
    biography TEXT,
    population TEXT,
    twitter_url TEXT,
    instagram_url TEXT,
    facebook_url TEXT,
    youtube_url TEXT,
    other_url TEXT,
    FOREIGN KEY (bioguide_id) REFERENCES bicam.members (bioguide_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (congress) REFERENCES bicam.congresses (congress_number)
        DEFERRABLE INITIALLY DEFERRED,
    PRIMARY KEY (bioguide_id, start_year, end_year)
);

CREATE TABLE IF NOT EXISTS bicam.members_leadership_roles(
    bioguide_id TEXT,
    role TEXT,
    congress INTEGER,
    chamber TEXT,
    is_current BOOLEAN,
    FOREIGN KEY (bioguide_id) REFERENCES bicam.members (bioguide_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (congress) REFERENCES bicam.congresses (congress_number)
        DEFERRABLE INITIALLY DEFERRED,
    UNIQUE (bioguide_id, role, congress, chamber)
);

CREATE TABLE IF NOT EXISTS bicam.members_party_history(
    bioguide_id TEXT,
    party_code TEXT,
    party_name TEXT,
    start_year INTEGER,
    end_year INTEGER,
    FOREIGN KEY (bioguide_id) REFERENCES bicam.members (bioguide_id)
        DEFERRABLE INITIALLY DEFERRED,
    UNIQUE (bioguide_id, party_code, start_year, end_year)
);

-- nominations related
CREATE TABLE IF NOT EXISTS bicam.nominations_actions(
    action_id TEXT PRIMARY KEY,
    nomination_id TEXT,
    action_code TEXT,
    action_type TEXT,
    action_date DATE,
    text TEXT,
    FOREIGN KEY (nomination_id) REFERENCES bicam.nominations (nomination_id)
        DEFERRABLE INITIALLY DEFERRED,
    UNIQUE (action_id, nomination_id)
);

CREATE TABLE IF NOT EXISTS bicam.nominations_positions(
    nomination_id TEXT,
    ordinal INTEGER,
    position_title TEXT,
    organization TEXT,
    intro_text TEXT,
    nominee_count INTEGER,
    FOREIGN KEY (nomination_id) REFERENCES bicam.nominations (nomination_id)
        DEFERRABLE INITIALLY DEFERRED,
    PRIMARY KEY (nomination_id, ordinal)
);

CREATE TABLE IF NOT EXISTS bicam.nominations_actions_committees(
    action_id TEXT,
    nomination_id TEXT,
    committee_code TEXT,
    FOREIGN KEY (action_id, nomination_id) REFERENCES bicam.nominations_actions (action_id, nomination_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (committee_code) REFERENCES bicam.committees (committee_code)
        DEFERRABLE INITIALLY DEFERRED,
    PRIMARY KEY (action_id, nomination_id, committee_code)
);

CREATE TABLE IF NOT EXISTS bicam.nominations_committeeactivities(
    nomination_id TEXT,
    committee_code TEXT,
    activity_name TEXT,
    activity_date TIMESTAMP WITH TIME ZONE,
    FOREIGN KEY (nomination_id) REFERENCES bicam.nominations (nomination_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (committee_code) REFERENCES bicam.committees (committee_code)
        DEFERRABLE INITIALLY DEFERRED,
    UNIQUE (nomination_id, committee_code, activity_date, activity_name)
);

CREATE TABLE IF NOT EXISTS bicam.nominations_associated_hearings(
    nomination_id TEXT,
    hearing_id TEXT,
    FOREIGN KEY (nomination_id) REFERENCES bicam.nominations (nomination_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (hearing_id) REFERENCES bicam.hearings (hearing_id)
        DEFERRABLE INITIALLY DEFERRED,
    PRIMARY KEY (nomination_id, hearing_id)
);

CREATE TABLE IF NOT EXISTS bicam.nominations_nominees(
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
    FOREIGN KEY (nomination_id, ordinal) REFERENCES bicam.nominations_positions (nomination_id, ordinal)
        DEFERRABLE INITIALLY DEFERRED,
    UNIQUE (nomination_id, ordinal, first_name, middle_name, last_name)
);

-- treaties related
CREATE TABLE IF NOT EXISTS bicam.treaties_actions(
    action_id TEXT PRIMARY KEY,
    treaty_id TEXT,
    action_code TEXT,
    action_date DATE,
    text TEXT,
    action_type TEXT,
    FOREIGN KEY (treaty_id) REFERENCES bicam.treaties (treaty_id)
        DEFERRABLE INITIALLY DEFERRED,
    UNIQUE (action_id, treaty_id)
);

CREATE TABLE IF NOT EXISTS bicam.treaties_actions_committees(
    action_id TEXT,
    treaty_id TEXT,
    committee_code TEXT,
    FOREIGN KEY (action_id, treaty_id) REFERENCES bicam.treaties_actions (action_id, treaty_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (committee_code) REFERENCES bicam.committees (committee_code)
        DEFERRABLE INITIALLY DEFERRED,
    PRIMARY KEY (action_id, treaty_id, committee_code)
);

CREATE TABLE IF NOT EXISTS bicam.treaties_country_parties(
    treaty_id TEXT,
    country TEXT,
    FOREIGN KEY (treaty_id) REFERENCES bicam.treaties (treaty_id)
        DEFERRABLE INITIALLY DEFERRED,
    PRIMARY KEY (treaty_id, country)
);

CREATE TABLE IF NOT EXISTS bicam.treaties_index_terms(
    treaty_id TEXT,
    index_term TEXT,
    FOREIGN KEY (treaty_id) REFERENCES bicam.treaties (treaty_id)
        DEFERRABLE INITIALLY DEFERRED,
    PRIMARY KEY (treaty_id, index_term)
);

CREATE TABLE IF NOT EXISTS bicam.treaties_titles(
    treaty_id TEXT,
    title TEXT,
    title_type TEXT,
    FOREIGN KEY (treaty_id) REFERENCES bicam.treaties (treaty_id)
        DEFERRABLE INITIALLY DEFERRED,
    UNIQUE (treaty_id, title, title_type)
);

-- committeemeetings related (depends on bills, treaties, nominations, hearings)
CREATE TABLE IF NOT EXISTS bicam.committeemeetings_associated_bills(
    meeting_id TEXT,
    bill_id TEXT,
    FOREIGN KEY (meeting_id) REFERENCES bicam.committeemeetings (meeting_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (bill_id) REFERENCES bicam.bills (bill_id)
        DEFERRABLE INITIALLY DEFERRED,
    PRIMARY KEY (meeting_id, bill_id)
);

CREATE TABLE IF NOT EXISTS bicam.committeemeetings_associated_treaties(
    meeting_id TEXT,
    treaty_id TEXT,
    FOREIGN KEY (meeting_id) REFERENCES bicam.committeemeetings (meeting_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (treaty_id) REFERENCES bicam.treaties (treaty_id)
        DEFERRABLE INITIALLY DEFERRED,
    PRIMARY KEY (meeting_id, treaty_id)
);

CREATE TABLE IF NOT EXISTS bicam.committeemeetings_associated_nominations(
    meeting_id TEXT,
    nomination_id TEXT,
    FOREIGN KEY (meeting_id) REFERENCES bicam.committeemeetings (meeting_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (nomination_id) REFERENCES bicam.nominations (nomination_id)
        DEFERRABLE INITIALLY DEFERRED,
    PRIMARY KEY (meeting_id, nomination_id)
);

CREATE TABLE IF NOT EXISTS bicam.committeemeetings_meeting_documents(
    meeting_id TEXT,
    name TEXT,
    document_type TEXT,
    description TEXT,
    url TEXT,
    FOREIGN KEY (meeting_id) REFERENCES bicam.committeemeetings (meeting_id)
        DEFERRABLE INITIALLY DEFERRED,
    UNIQUE (meeting_id, url)
);

CREATE TABLE IF NOT EXISTS bicam.committeemeetings_witness_documents(
    meeting_id TEXT,
    document_type TEXT,
    url TEXT,
    FOREIGN KEY (meeting_id) REFERENCES bicam.committeemeetings (meeting_id)
        DEFERRABLE INITIALLY DEFERRED,
    UNIQUE (meeting_id, url)
);

CREATE TABLE IF NOT EXISTS bicam.committeemeetings_witnesses(
    meeting_id TEXT,
    name TEXT,
    position TEXT,
    organization TEXT,
    FOREIGN KEY (meeting_id) REFERENCES bicam.committeemeetings (meeting_id)
        DEFERRABLE INITIALLY DEFERRED,
    UNIQUE (meeting_id, name, position, organization)
);

CREATE TABLE IF NOT EXISTS bicam.committeemeetings_associated_hearings(
    meeting_id TEXT,
    hearing_id TEXT,
    FOREIGN KEY (meeting_id) REFERENCES bicam.committeemeetings (meeting_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (hearing_id) REFERENCES bicam.hearings (hearing_id)
        DEFERRABLE INITIALLY DEFERRED,
    PRIMARY KEY (meeting_id, hearing_id)
);

CREATE TABLE IF NOT EXISTS bicam.committeemeetings_committees(
    meeting_id TEXT,
    committee_code TEXT,
    FOREIGN KEY (meeting_id) REFERENCES bicam.committeemeetings (meeting_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (committee_code) REFERENCES bicam.committees (committee_code)
        DEFERRABLE INITIALLY DEFERRED,
    PRIMARY KEY (meeting_id, committee_code)
);

CREATE TABLE IF NOT EXISTS bicam.bills_lobbied(
    filing_uuid TEXT,
    general_isue_code TEXT,
    bill_id TEXT,
    is_implementation BOOLEAN,
);

-- Finish off with granting permissions
DO $$
DECLARE
    current_table_name text;
    current_schema_name text;
BEGIN
    FOR current_schema_name IN SELECT schema_name FROM information_schema.schemata WHERE schema_name IN ('_staging_congressional', 'congressional') LOOP
        FOR current_table_name IN SELECT table_name FROM information_schema.tables WHERE table_schema = current_schema_name LOOP
            EXECUTE format('GRANT ALL ON TABLE %I.%I TO postgres_admin__ryan', current_schema_name, current_table_name);
        END LOOP;
    END LOOP;
END$$;

END TRANSACTION;

SET CONSTRAINTS ALL IMMEDIATE;
COMMIT;

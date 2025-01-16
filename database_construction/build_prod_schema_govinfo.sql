DROP SCHEMA IF EXISTS govinfo CASCADE;
CREATE SCHEMA IF NOT EXISTS govinfo;

BEGIN;

SET CONSTRAINTS ALL DEFERRED;

CREATE TABLE IF NOT EXISTS govinfo.bills(
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
CREATE TABLE govinfo.ref_bill_version_codes (
    version_code TEXT PRIMARY KEY,
    version_name  TEXT,
    description TEXT NOT NULL,
    chamber TEXT
);

-- Insert data
INSERT INTO govinfo.ref_bill_version_codes (version_name, version_code, description, chamber) VALUES
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


CREATE TABLE IF NOT EXISTS govinfo.bills_reference_codes(
    bill_code_id TEXT PRIMARY KEY,
    package_id TEXT,
    reference_code TEXT, -- combination of label and title
    FOREIGN KEY (package_id) REFERENCES govinfo.bills(package_id)
    INITIALLY DEFERRED
);

CREATE TABLE IF NOT EXISTS govinfo.bills_reference_codes_sections(
    bill_code_id TEXT,
    code_section TEXT,
    FOREIGN KEY (bill_code_id) REFERENCES govinfo.bills_reference_codes(bill_code_id)
    INITIALLY DEFERRED,
    UNIQUE (bill_code_id, code_section)
);

CREATE TABLE IF NOT EXISTS govinfo.bills_reference_laws(
    package_id TEXT,
    law_id TEXT, -- fix law_id
    law_type TEXT,
    FOREIGN KEY (package_id) REFERENCES govinfo.bills(package_id)
    INITIALLY DEFERRED,
    PRIMARY KEY (package_id, law_id)
);

CREATE TABLE IF NOT EXISTS govinfo.bills_reference_statutes(
    bill_statute_id TEXT PRIMARY KEY,
    package_id TEXT,
    reference_statute TEXT, -- combine label and title
    FOREIGN KEY (package_id) REFERENCES govinfo.bills(package_id)
    INITIALLY DEFERRED
);

CREATE TABLE IF NOT EXISTS govinfo.bills_reference_statutes_pages(
    bill_statute_id TEXT,
    page TEXT, -- combine label and title
    FOREIGN KEY (bill_statute_id) REFERENCES govinfo.bills_reference_statutes(bill_statute_id)
    INITIALLY DEFERRED,
    UNIQUE (bill_statute_id, page)
);

CREATE TABLE IF NOT EXISTS govinfo.bills_short_titles(
    package_id TEXT,
    short_title TEXT,
    level TEXT,
    type TEXT,
    FOREIGN KEY (package_id) REFERENCES govinfo.bills(package_id)
    INITIALLY DEFERRED,
    UNIQUE (package_id, short_title, level, type)
);

CREATE TABLE IF NOT EXISTS govinfo.committeeprints(
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

CREATE TABLE IF NOT EXISTS govinfo.committeeprints_granules(
    granule_id TEXT,
    package_id TEXT,
    FOREIGN KEY (package_id) REFERENCES govinfo.committeeprints(package_id)
    INITIALLY DEFERRED,
    PRIMARY KEY (granule_id, package_id)
);

CREATE TABLE IF NOT EXISTS govinfo.committeeprints_committees(
    package_id TEXT,
    granule_id TEXT,
    committee_code TEXT,
    committee_name TEXT,
    chamber TEXT,
    FOREIGN KEY (package_id, granule_id) REFERENCES govinfo.committeeprints_granules(granule_id, package_id),
    UNIQUE (package_id, granule_id, committee_code, committee_name, chamber)
);

CREATE TABLE IF NOT EXISTS govinfo.committeeprints_reference_bills(
    package_id TEXT,
    granule_id TEXT,
    bill_id TEXT,
    FOREIGN KEY (package_id, granule_id) REFERENCES govinfo.committeeprints_granules(granule_id, package_id),
    UNIQUE (package_id, granule_id, bill_id)
);

CREATE TABLE IF NOT EXISTS govinfo.committeereports(
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

CREATE TABLE IF NOT EXISTS govinfo.committeereports_granules(
    granule_id TEXT,
    package_id TEXT,
    FOREIGN KEY (package_id) REFERENCES govinfo.committeereports(package_id)
    INITIALLY DEFERRED,
    PRIMARY KEY (granule_id, package_id)
);

CREATE TABLE IF NOT EXISTS govinfo.committeereports_committees(
    granule_id TEXT,
    package_id TEXT,
    committee_code TEXT,
    committee_name TEXT,
    chamber TEXT,
    FOREIGN KEY (package_id, granule_id) REFERENCES govinfo.committeereports_granules(granule_id, package_id),
    UNIQUE (package_id, granule_id, committee_code, committee_name, chamber)
);

CREATE TABLE IF NOT EXISTS govinfo.committeereports_members(
    granule_id TEXT,
    package_id TEXT,
    bioguide_id TEXT,
    FOREIGN KEY (package_id, granule_id) REFERENCES govinfo.committeereports_granules(granule_id, package_id),
    UNIQUE (package_id, granule_id, bioguide_id)

);

CREATE TABLE IF NOT EXISTS govinfo.committeereports_reference_bills(
    granule_id TEXT,
    package_id TEXT,
    bill_id TEXT,
    FOREIGN KEY (package_id, granule_id) REFERENCES govinfo.committeereports_granules(granule_id, package_id),
    UNIQUE (package_id, granule_id, bill_id)
);


CREATE TABLE IF NOT EXISTS govinfo.hearings(
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

CREATE TABLE IF NOT EXISTS govinfo.hearings_granules(
    granule_id TEXT,
    package_id TEXT,
    FOREIGN KEY (package_id) REFERENCES govinfo.hearings(package_id)
    INITIALLY DEFERRED,
    PRIMARY KEY (granule_id, package_id)
);

CREATE TABLE IF NOT EXISTS govinfo.hearings_committees(
    granule_id TEXT,
    package_id TEXT,
    committee_code TEXT,
    committee_name TEXT,
    FOREIGN KEY (package_id, granule_id) REFERENCES govinfo.hearings_granules(granule_id, package_id),
    UNIQUE (package_id, granule_id, committee_code, committee_name)
);

CREATE TABLE IF NOT EXISTS govinfo.hearings_members(
    granule_id TEXT,
    package_id TEXT,
    bioguide_id TEXT,
    name TEXT,
    FOREIGN KEY (package_id, granule_id) REFERENCES govinfo.hearings_granules(granule_id, package_id),
    UNIQUE (package_id, granule_id, bioguide_id, name)
);

CREATE TABLE IF NOT EXISTS govinfo.hearings_reference_bills(
    granule_id TEXT,
    package_id TEXT,
    bill_id TEXT,
    FOREIGN KEY (package_id, granule_id) REFERENCES govinfo.hearings_granules(granule_id, package_id),
    UNIQUE (package_id, granule_id, bill_id)
);

CREATE TABLE IF NOT EXISTS govinfo.hearings_witnesses(
    granule_id TEXT,
    witness TEXT,
    UNIQUE (granule_id, witness)
);

CREATE TABLE IF NOT EXISTS govinfo.treaties(
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

CREATE TABLE IF NOT EXISTS govinfo.treaties_granules(
    granule_id TEXT,
    package_id TEXT,
    FOREIGN KEY (package_id) REFERENCES treaties(package_id)
    INITIALLY DEFERRED,
    PRIMARY KEY (granule_id, package_id)
);

CREATE TABLE IF NOT EXISTS govinfo.treaties_committees(
    granule_id TEXT,
    package_id TEXT,
    committee_code TEXT,
    committee_name TEXT,
    chamber TEXT,
    FOREIGN KEY (package_id, granule_id) REFERENCES treaties_granules(granule_id, package_id),
    UNIQUE (package_id, granule_id, committee_code, committee_name, chamber)
);

CREATE TABLE IF NOT EXISTS govinfo.congressional_directories(
    package_id TEXT PRIMARY KEY,
    title TEXT,
    congress INTEGER,
    issued_at DATE,
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

CREATE TABLE IF NOT EXISTS govinfo.congressional_directories_isbn(
    package_id TEXT,
    isbn TEXT,
    FOREIGN KEY (package_id) REFERENCES govinfo.congressional_directories(package_id),
    UNIQUE(package_id, isbn)
);

CREATE TABLE IF NOT EXISTS govinfo.members(
    granule_id TEXT,
    package_id TEXT,
    bioguide_id TEXT,
    title TEXT,
    biography TEXT,
    member_type TEXT,
    population TEXT,
    gpo_id TEXT,
    authority_id TEXT,
    official_url TEXT,
    twitter_url TEXT,
    instagram_url TEXT,
    facebook_url TEXT,
    youtube_url TEXT,
    other_url TEXT,
    FOREIGN KEY (package_id) REFERENCES govinfo.congressional_directories(package_id)
    INITIALLY DEFERRED,
    PRIMARY KEY (granule_id, package_id)
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
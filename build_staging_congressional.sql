CREATE TABLE bicam_staging_congressional.amendments (
    type text,
    number text,
    actions_url text,
    actions_count text,
    chamber text,
    purpose text,
    congress text,
    updatedate text,
    amendedbill_url text,
    amendedbill_type text,
    amendedbill_title text,
    amendedbill_number text,
    amendedbill_congress text,
    amendedbill_originchamber text,
    amendedbill_originchambercode text,
    amendedbill_updatedateincludingtext text,
    description text,
    latestaction_text text,
    latestaction_actiondate text,
    latestaction_actiontime text,
    submitteddate text,
    amendedamendment_url text,
    amendedamendment_type text,
    amendedamendment_number text,
    amendedamendment_purpose text,
    amendedamendment_congress text,
    amendedamendment_updatedate text,
    amendedamendment_description text,
    amendment_id text,
    processed_at text,
    source_doc_id text,
    amendmentstoamendment_url text,
    amendmentstoamendment_count text,
    textversions_url text,
    textversions_count text,
    notes text,
    proposeddate text,
    cosponsors_url text,
    cosponsors_count text,
    cosponsors_countincludingwithdrawncosponsors text,
    onbehalfofsponsor text,
    amendedtreaty_url text,
    amendedtreaty_congress text,
    amendedtreaty_treatynumber text
);

CREATE TABLE bicam_staging_congressional.amendments_actions (
    text text,
    type text,
    actioncode text,
    actiondate text,
    actiontime text,
    sourcesystem_code text,
    sourcesystem_name text,
    amendments_id text,
    amendment_id text,
    id text,
    processed_at text,
    source_doc_id text,
    committees text,
    sourcesystem text
);

CREATE TABLE bicam_staging_congressional.amendments_actions_committees (
    url text,
    name text,
    systemcode text,
    id text,
    amendment_id text,
    list_index text
);

CREATE TABLE IF NOT EXISTS bicam_staging_congressional.amendments_actions_recorded_votes (
    action_id TEXT,
    amendment_id TEXT,
    chamber TEXT,
    congress INTEGER,
    date TIMESTAMP WITH TIME ZONE,
    roll_number INTEGER,
    session INTEGER,
    url TEXT,
    UNIQUE (action_id, amendment_id, url)
);

CREATE TABLE bicam_staging_congressional.amendments_cosponsors (
    party text,
    state text,
    fullname text,
    lastname text,
    firstname text,
    bioguideid text,
    middlename text,
    amendments_id text,
    sponsorshipdate text,
    isoriginalcosponsor text,
    amendment_id text,
    id text,
    processed_at text,
    source_doc_id text,
    district text,
    sponsorshipwithdrawndate text
);

CREATE TABLE bicam_staging_congressional.amendments_latestaction_links (
    url text,
    name text,
    id text,
    amendment_id text,
    list_index text
);

CREATE TABLE bicam_staging_congressional.amendments_links (
    url text,
    name text,
    id text,
    amendment_id text,
    list_index text
);

CREATE TABLE bicam_staging_congressional.amendments_notes (
    text text,
    links text,
    id text,
    amendment_id text,
    list_index text
);

CREATE TABLE bicam_staging_congressional.amendments_notes_links (
    url text,
    name text,
    id text,
    notes_id text,
    list_index text
);

CREATE TABLE bicam_staging_congressional.amendments_onbehalfofsponsor (
    url text,
    type text,
    party text,
    state text,
    fullname text,
    lastname text,
    firstname text,
    bioguideid text,
    id text,
    amendment_id text,
    list_index text
);

CREATE TABLE bicam_staging_congressional.amendments_sponsors (
    url text,
    party text,
    state text,
    district text,
    fullname text,
    lastname text,
    firstname text,
    bioguideid text,
    middlename text,
    id text,
    amendment_id text,
    list_index text,
    name text
);

CREATE TABLE bicam_staging_congressional.bills (
    type text,
    title text,
    number text,
    titles_url text,
    titles_count text,
    actions_url text,
    actions_count text,
    congress text,
    subjects_url text,
    subjects_count text,
    summaries_url text,
    summaries_count text,
    committees_url text,
    committees_count text,
    cosponsors_url text,
    cosponsors_count text,
    cosponsors_countincludingwithdrawncosponsors text,
    policyarea_name text,
    updatedate text,
    latestaction_text text,
    latestaction_actiondate text,
    textversions_url text,
    textversions_count text,
    originchamber text,
    introduceddate text,
    originchambercode text,
    updatedateincludingtext text,
    bill_id text,
    processed_at text,
    source_doc_id text,
    relatedbills_url text,
    relatedbills_count text,
    amendments_url text,
    amendments_count text,
    latestaction_actiontime text,
    constitutionalauthoritystatementtext text,
    notes text
);

CREATE TABLE bicam_staging_congressional.bills_actions (
    text text,
    type text,
    bills_id text,
    actioncode text,
    actiondate text,
    sourcesystem_code text,
    sourcesystem_name text,
    bill_id text,
    id text,
    processed_at text,
    source_doc_id text,
    actiontime text,
    calendarnumber_number text,
    calendarnumber_calendar text
);

CREATE TABLE bicam_staging_congressional.bills_actions_committees (
    url text,
    name text,
    systemcode text,
    id text,
    bill_id text,
    list_index text
);

CREATE TABLE bicam_staging_congressional.bills_actions_recordedvotes (
    url text,
    date text,
    chamber text,
    congress text,
    rollnumber text,
    sessionnumber text,
    id text,
    bill_id text,
    list_index text
);

CREATE TABLE bicam_staging_congressional.bills_cbocostestimates (
    url text,
    title text,
    pubdate text,
    description text,
    id text,
    bill_id text,
    list_index text
);

CREATE TABLE bicam_staging_congressional.bills_committeereports (
    url text,
    citation text,
    id text,
    bill_id text,
    list_index text
);

CREATE TABLE bicam_staging_congressional.bills_cosponsors (
    party text,
    state text,
    bills_id text,
    district text,
    fullname text,
    lastname text,
    firstname text,
    bioguideid text,
    sponsorshipdate text,
    isoriginalcosponsor text,
    bill_id text,
    id text,
    processed_at text,
    source_doc_id text,
    middlename text,
    sponsorshipwithdrawndate text
);

CREATE TABLE bicam_staging_congressional.bills_laws (
    type text,
    number text,
    id text,
    bill_id text,
    list_index text
);

CREATE TABLE bicam_staging_congressional.bills_notes (
    text text,
    links text,
    id text,
    bill_id text,
    list_index text
);

CREATE TABLE bicam_staging_congressional.bills_notes_links (
    url text,
    name text,
    id text,
    bill_id text,
    list_index text
);

CREATE TABLE bicam_staging_congressional.bills_relatedbills (
    bill_id text,
    bills_id text,
    relatedbill_id text,
    relationship_type text,
    relationship_identified_by text,
    id text,
    processed_at text,
    source_doc_id text
);

CREATE TABLE bicam_staging_congressional.bills_sponsors (
    url text,
    party text,
    state text,
    district text,
    fullname text,
    lastname text,
    firstname text,
    bioguideid text,
    isbyrequest text,
    id text,
    bill_id text,
    list_index text,
    middlename text
);

CREATE TABLE bicam_staging_congressional.bills_subjects (
    name text,
    bills_id text,
    updatedate text,
    bill_id text,
    id text,
    processed_at text,
    source_doc_id text
);

CREATE TABLE bicam_staging_congressional.bills_summaries (
    text text,
    bills_id text,
    actiondate text,
    actiondesc text,
    updatedate text,
    versioncode text,
    bill_id text,
    id text,
    processed_at text,
    source_doc_id text
);

CREATE TABLE bicam_staging_congressional.bills_titles (
    title text,
    bills_id text,
    titletype text,
    updatedate text,
    titletypecode text,
    bill_id text,
    id text,
    processed_at text,
    source_doc_id text,
    billtextversioncode text,
    billtextversionname text,
    chambercode text,
    chambername text,
    sourcesystem_code text,
    sourcesystem_name text
);

CREATE TABLE bicam_staging_congressional.committeemeetings (
    date text,
    type text,
    title text,
    chamber text,
    eventid text,
    congress text,
    location_room text,
    location_building text,
    updatedate text,
    meetingstatus text,
    meeting_id text,
    processed_at text,
    source_doc_id text,
    location_address text,
    relateditems_nominations text,
    relateditems_treaties text
);

CREATE TABLE bicam_staging_congressional.committeemeetings_committees (
    url text,
    name text,
    systemcode text,
    id text,
    meeting_id text,
    list_index text
);

CREATE TABLE bicam_staging_congressional.committeemeetings_hearingtranscript (
    url text,
    jacketnumber text,
    id text,
    meeting_id text,
    list_index text
);

CREATE TABLE bicam_staging_congressional.committeemeetings_meetingdocuments (
    url text,
    name text,
    format text,
    documenttype text,
    id text,
    meeting_id text,
    list_index text,
    description text
);

CREATE TABLE bicam_staging_congressional.committeemeetings_relateditems_bills (
    url text,
    type text,
    number text,
    congress text,
    id text,
    meeting_id text,
    list_index text
);

CREATE TABLE bicam_staging_congressional.committeemeetings_videos (
    url text,
    name text,
    id text,
    meeting_id text,
    list_index text
);

CREATE TABLE bicam_staging_congressional.committeemeetings_witnessdocuments (
    url text,
    format text,
    documenttype text,
    id text,
    meeting_id text,
    list_index text
);

CREATE TABLE bicam_staging_congressional.committeemeetings_witnesses (
    name text,
    position text,
    organization text,
    id text,
    meeting_id text,
    list_index text
);

CREATE TABLE bicam_staging_congressional.committeeprints (
    text_url text,
    text_count text,
    title text,
    chamber text,
    citation text,
    congress text,
    updatedate text,
    jacketnumber text,
    print_id text,
    processed_at text,
    source_doc_id text,
    number text
);

CREATE TABLE bicam_staging_congressional.committeeprints_associatedbills (
    url text,
    type text,
    number text,
    congress text,
    id text,
    print_id text,
    list_index text
);

CREATE TABLE bicam_staging_congressional.committeeprints_committees (
    url text,
    name text,
    systemcode text,
    id text,
    print_id text,
    list_index text
);

CREATE TABLE bicam_staging_congressional.committeereports (
    part text,
    text_url text,
    text_count text,
    type text,
    title text,
    number text,
    chamber text,
    citation text,
    congress text,
    issuedate text,
    reporttype text,
    updatedate text,
    sessionnumber text,
    isconferencereport text,
    report_id text,
    processed_at text,
    source_doc_id text,
    committees text,
    associatedtreaties text
);

CREATE TABLE bicam_staging_congressional.committeereports_associatedbill (
    url text,
    type text,
    number text,
    congress text,
    id text,
    report_id text,
    list_index text
);

CREATE TABLE bicam_staging_congressional.committeereports_associatedtreaties (
    url text,
    number text,
    congress text,
    id text,
    report_id text,
    list_index text,
    part text
);

CREATE TABLE bicam_staging_congressional.committeereports_committees (
    url text,
    name text,
    systemcode text,
    id text,
    report_id text,
    list_index text
);

CREATE TABLE bicam_staging_congressional.committees (
    type text,
    reports_url text,
    reports_count text,
    iscurrent text,
    systemcode text,
    updatedate text,
    communications_url text,
    communications_count text,
    committee_code text,
    processed_at text,
    source_doc_id text,
    parent_url text,
    parent_name text,
    parent_systemcode text,
    bills_url text,
    bills_count text,
    nominations_url text,
    nominations_count text
);

CREATE TABLE bicam_staging_congressional.committees_committeereports (
    part text,
    type text,
    number text,
    chamber text,
    citation text,
    congress text,
    updatedate text,
    committees_id text,
    committee_code text,
    id text,
    processed_at text,
    source_doc_id text
);

CREATE TABLE bicam_staging_congressional.committees_history (
    enddate text,
    startdate text,
    updatedate text,
    officialname text,
    libraryofcongressname text,
    id text,
    committee_code text,
    list_index text,
    committeetypecode text,
    establishingauthority text,
    loclinkeddataid text,
    superintendentdocumentnumber text,
    naraid text
);

CREATE TABLE bicam_staging_congressional.committees_subcommittees (
    url text,
    name text,
    systemcode text,
    id text,
    committee_code text,
    list_index text
);

CREATE TABLE bicam_staging_congressional.congresses (
    url text,
    name text,
    number text,
    endyear text,
    batch_id text,
    startyear text,
    updatedate text,
    congress_number text,
    processed_at text,
    source_doc_id text
);

CREATE TABLE bicam_staging_congressional.congresses_sessions (
    type text,
    number text,
    chamber text,
    startdate text,
    id text,
    congress_number text,
    list_index text,
    enddate text
);

CREATE TABLE bicam_staging_congressional.hearings (
    title text,
    chamber text,
    citation text,
    congress text,
    updatedate text,
    jacketnumber text,
    libraryofcongressidentifier text,
    hearing_id text,
    processed_at text,
    source_doc_id text,
    associatedmeeting_url text,
    associatedmeeting_eventid text,
    part text,
    number text
);

CREATE TABLE bicam_staging_congressional.hearings_committees (
    url text,
    name text,
    systemcode text,
    id text,
    hearing_id text,
    list_index text
);

CREATE TABLE bicam_staging_congressional.hearings_dates (
    date text,
    id text,
    hearing_id text,
    list_index text
);

CREATE TABLE bicam_staging_congressional.hearings_formats (
    url text,
    type text,
    id text,
    hearing_id text,
    list_index text
);

CREATE TABLE bicam_staging_congressional.members (
    state text,
    batch_id text,
    district text,
    lastname text,
    birthyear text,
    depiction_imageurl text,
    depiction_attribution text,
    firstname text,
    bioguideid text,
    middlename text,
    updatedate text,
    bioguide_id text,
    currentmember text,
    directordername text,
    invertedordername text,
    sponsoredlegislation_url text,
    sponsoredlegislation_count text,
    cosponsoredlegislation_url text,
    cosponsoredlegislation_count text,
    processed_at text,
    source_doc_id text,
    honorificname text,
    addressinformation_city text,
    addressinformation_zipcode text,
    addressinformation_district text,
    addressinformation_phonenumber text,
    addressinformation_officeaddress text,
    officialwebsiteurl text,
    deathyear text,
    nickname text,
    suffixname text
);

CREATE TABLE bicam_staging_congressional.members_leadership (
    type text,
    congress text,
    id text,
    bioguide_id text,
    list_index text,
    current text
);

CREATE TABLE bicam_staging_congressional.members_partyhistory (
    partyname text,
    startyear text,
    partyabbreviation text,
    id text,
    bioguide_id text,
    list_index text,
    endyear text
);

CREATE TABLE bicam_staging_congressional.members_terms (
    chamber text,
    endyear text,
    congress text,
    district text,
    startyear text,
    statecode text,
    statename text,
    membertype text,
    id text,
    bioguide_id text,
    list_index text
);

CREATE TABLE bicam_staging_congressional.nominations (
    islist text,
    number text,
    actions_url text,
    actions_count text,
    citation text,
    congress text,
    committees_url text,
    committees_count text,
    partnumber text,
    updatedate text,
    latestaction_text text,
    latestaction_actiondate text,
    receiveddate text,
    nominationtype_iscivilian text,
    nomination_id text,
    processed_at text,
    source_doc_id text,
    authoritydate text,
    nominationtype_ismilitary text,
    executivecalendarnumber text,
    description text,
    hearings_url text,
    hearings_count text,
    isprivileged text
);

CREATE TABLE bicam_staging_congressional.nominations_actions (
    text text,
    type text,
    actioncode text,
    actiondate text,
    nominations_id text,
    action_id text,
    id text,
    processed_at text,
    source_doc_id text
);

CREATE TABLE bicam_staging_congressional.nominations_actions_committees (
    url text,
    name text,
    systemcode text,
    id text,
    action_id text,
    list_index text
);

CREATE TABLE bicam_staging_congressional.nominations_committeeactivities (
    name text,
    type text,
    chamber text,
    activity_date text,
    activity_name text,
    committee_code text,
    nominations_id text,
    nomination_id text,
    id text,
    processed_at text,
    source_doc_id text
);

CREATE TABLE bicam_staging_congressional.nominations_hearings (
    date text,
    number text,
    chamber text,
    citation text,
    partnumber text,
    jacketnumber text,
    nominations_id text,
    nomination_id text,
    id text,
    processed_at text,
    source_doc_id text
);

CREATE TABLE bicam_staging_congressional.nominations_individualnominees (
    state text,
    ordinal text,
    lastname text,
    firstname text,
    middlename text,
    position_id text,
    nomination_id text,
    nominations_id text,
    id text,
    processed_at text,
    source_doc_id text,
    suffix text,
    predecessorname text,
    prefix text,
    effectivedate text,
    corpscode text
);

CREATE TABLE bicam_staging_congressional.nominations_nominees (
    url text,
    ordinal text,
    introtext text,
    nomineecount text,
    organization text,
    id text,
    nomination_id text,
    list_index text,
    positiontitle text,
    division text
);

CREATE TABLE bicam_staging_congressional.treaties (
    topic text,
    number text,
    suffix text,
    actions_url text,
    actions_count text,
    batch_id text,
    oldnumber text,
    treaty_id text,
    updatedate text,
    inforcedate text,
    relateddocs text,
    resolutiontext text,
    transmitteddate text,
    congressreceived text,
    congressconsidered text,
    oldnumberdisplayname text,
    processed_at text,
    source_doc_id text,
    parts_count text
);

CREATE TABLE bicam_staging_congressional.treaties_actions (
    text text,
    type text,
    batch_id text,
    committee text,
    treaty_id text,
    actioncode text,
    actiondate text,
    action_id text,
    id text,
    processed_at text,
    source_doc_id text,
    committee_url text,
    committee_name text,
    committee_systemcode text
);

CREATE TABLE bicam_staging_congressional.treaties_countriesparties (
    name text,
    id text,
    treaty_id text,
    list_index text
);

CREATE TABLE bicam_staging_congressional.treaties_indexterms (
    name text,
    id text,
    treaty_id text,
    list_index text
);

CREATE TABLE bicam_staging_congressional.treaties_parts_urls (
    value text,
    id text,
    treaty_id text,
    list_index text
);

CREATE TABLE bicam_staging_congressional.treaties_relateddocs (
    url text,
    citation text,
    id text,
    treaty_id text,
    list_index text
);

CREATE TABLE bicam_staging_congressional.treaties_titles (
    title text,
    titletype text,
    id text,
    treaty_id text,
    list_index text
);

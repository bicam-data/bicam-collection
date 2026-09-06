"""Load a tiny, deterministic raw corpus for CI's database-backed dbt build."""

from __future__ import annotations

import hashlib
import json
import os
import uuid

import psycopg


def _fixture_rows() -> list[tuple[str, str, str, str, dict, str | None]]:
    return [
        (
            "https://api.congress.gov/v3/bill/118/hr/1?format=json",
            "congress",
            "bills",
            "bill_detail",
            {
                "bill": {
                    "type": "HR",
                    "number": "1",
                    "congress": 118,
                    "title": "A bill for the people",
                    "originChamber": "House",
                    "introducedDate": "2023-01-03",
                    "updateDate": "2023-01-04T00:00:00Z",
                    "actions": {"count": 1},
                    "amendments": {"count": 1},
                    "committees": {"count": 1},
                    "committeeReports": [
                        {
                            "type": "HRPT",
                            "number": 1,
                            "part": 1,
                            "congress": 118,
                            "citation": "H. Rept. 118-1",
                        }
                    ],
                    "cosponsors": {"count": 0},
                    "relatedBills": {"count": 1},
                    "subjects": {"count": 0},
                    "summaries": {"count": 0},
                    "textVersions": {"count": 1},
                    "titles": {"count": 2},
                }
            },
            None,
        ),
        (
            "https://api.congress.gov/v3/bill/118/hr/1/titles?format=json",
            "congress",
            "bills",
            "bill_titles",
            {
                "titles": [
                    {
                        "title": "A bill for the people",
                        "titleType": "Official Title as Introduced",
                        "chamberName": "House",
                        "titleTypeCode": "OL title",
                        "billTextVersionName": "Introduced in House",
                        "billTextVersionCode": "IH",
                        "updateDate": "2023-01-04T00:00:00Z",
                    },
                    {
                        "title": "A bill for the people",
                        "titleType": "Official Title as Introduced",
                        "chamberName": "House",
                        "titleTypeCode": "OL title",
                        "updateDate": "2023-01-03T00:00:00Z",
                    },
                ]
            },
            "https://api.congress.gov/v3/bill/118/hr/1?format=json",
        ),
        (
            "https://api.congress.gov/v3/committee/house/hsag00?format=json",
            "congress",
            "committees",
            "committee_detail",
            {
                "committee": {
                    "systemCode": "HSAG",
                    "name": "Agriculture Committee",
                    "type": "Standing",
                    "chamber": "House",
                    "isCurrent": True,
                    "bills": {"count": 1},
                    "reports": {"count": 1},
                    "nominations": {"count": 1},
                    "communications": {"count": 1},
                    "updateDate": "2023-01-05T00:00:00Z",
                }
            },
            None,
        ),
        (
            "https://api.congress.gov/v3/member/A000001?format=json",
            "congress",
            "members",
            "member_detail",
            {
                "member": {
                    "bioguideId": "A000001",
                    "firstName": "Fixture",
                    "lastName": "Member",
                    "state": "CA",
                    "currentMember": True,
                    "partyHistory": [
                        {
                            "partyAbbreviation": "D",
                            "partyName": "Democratic",
                            "startYear": 2023,
                        }
                    ],
                    "sponsoredLegislation": {
                        "count": 1,
                        "url": "https://api.congress.gov/v3/member/A000001/sponsored-legislation?format=json",
                    },
                    "cosponsoredLegislation": {
                        "count": 1,
                        "url": "https://api.congress.gov/v3/member/A000001/cosponsored-legislation?format=json",
                    },
                    "updateDate": "2023-01-07T00:00:00Z",
                }
            },
            None,
        ),
        (
            "https://api.congress.gov/v3/member/A000001/sponsored-legislation?format=json",
            "congress",
            "members",
            "member_sponsored_legislation",
            {
                "sponsoredLegislation": [
                    {"type": "HR", "number": "1", "congress": 118},
                    {"type": "HR", "number": "1", "congress": 118},
                    {
                        "type": "SAMDT",
                        "amendmentNumber": "1",
                        "congress": 118,
                        "url": "https://api.congress.gov/v3/amendment/118/samdt/1?format=json",
                    },
                    {
                        "type": "SAMDT",
                        "amendmentNumber": "1",
                        "congress": 118,
                        "url": "https://api.congress.gov/v3/amendment/118/samdt/1?format=json",
                    },
                ]
            },
            "https://api.congress.gov/v3/member/A000001?format=json",
        ),
        (
            "https://api.congress.gov/v3/member/A000001/cosponsored-legislation?format=json",
            "congress",
            "members",
            "member_cosponsored_legislation",
            {
                "cosponsoredLegislation": [
                    {"type": "HR", "number": "2", "congress": 118},
                    {"type": "HR", "number": "2", "congress": 118},
                    {
                        "type": "SAMDT",
                        "amendmentNumber": "1",
                        "congress": 118,
                        "url": "https://api.congress.gov/v3/amendment/118/samdt/1?format=json",
                    },
                    {
                        "type": "SAMDT",
                        "amendmentNumber": "1",
                        "congress": 118,
                        "url": "https://api.congress.gov/v3/amendment/118/samdt/1?format=json",
                    },
                ]
            },
            "https://api.congress.gov/v3/member/A000001?format=json",
        ),
        (
            "https://api.congress.gov/v3/bill/118/hr/2?format=json",
            "congress",
            "bills",
            "bill_detail",
            {
                "bill": {
                    "type": "HR",
                    "number": "2",
                    "congress": 118,
                    "title": "A related bill",
                    "originChamber": "House",
                    "introducedDate": "2023-01-05",
                    "updateDate": "2023-01-05T00:00:00Z",
                    "actions": {"count": 0},
                    "amendments": {"count": 0},
                    "committees": {"count": 0},
                    "cosponsors": {"count": 0},
                    "relatedBills": {"count": 0},
                    "subjects": {"count": 0},
                    "summaries": {"count": 0},
                    "textVersions": {"count": 0},
                    "titles": {"count": 0},
                }
            },
            None,
        ),
        # A complete child generation followed by an explicit empty complete
        # response. Both transport URLs normalize to one logical collection;
        # the later empty generation must retire the earlier subject rather
        # than leaving a stale public row behind.
        (
            "https://api.congress.gov/v3/bill/118/hr/2/subjects?format=json&offset=0",
            "congress",
            "bills",
            "bill_subjects",
            {
                "subjects": [
                    {
                        "name": "Retired fixture subject",
                        "updateDate": "2023-01-05T00:00:00Z",
                    }
                ]
            },
            "https://api.congress.gov/v3/bill/118/hr/2?format=json",
        ),
        (
            "https://api.congress.gov/v3/bill/118/hr/2/subjects?format=json&limit=250",
            "congress",
            "bills",
            "bill_subjects",
            {"subjects": []},
            "https://api.congress.gov/v3/bill/118/hr/2?format=json",
        ),
        (
            "https://api.congress.gov/v3/bill/118/hr/1/actions?format=json",
            "congress",
            "bills",
            "bill_actions",
            {
                "actions": [
                    {
                        "actionCode": "H123",
                        "actionDate": "2023-01-05",
                        "text": "Passed by recorded vote",
                        "type": "Floor",
                        "committees": [
                            {"systemCode": "HSAG", "name": "Agriculture Committee"},
                            {"systemCode": "HSAG", "name": "Agriculture Committee (corrected)"},
                        ],
                        "recordedVotes": [
                            {
                                "chamber": "House",
                                "congress": 118,
                                "date": "2023-01-05",
                                "rollNumber": 183,
                                "sessionNumber": 1,
                                "url": "https://clerk.house.gov/Votes/2023183",
                            },
                            {
                                "chamber": "House",
                                "congress": 118,
                                "date": "2023-01-06",
                                "rollNumber": 184,
                                "sessionNumber": 1,
                                "url": "https://clerk.house.gov/Votes/2023184",
                            },
                        ],
                    },
                    {
                        "actionCode": "H123",
                        "actionDate": "2023-01-05",
                        "text": "Passed by recorded vote",
                        "type": "Floor",
                        "committees": [
                            {"systemCode": "HSAG", "name": "Agriculture Committee"},
                            {"systemCode": "HSAG", "name": "Agriculture Committee (corrected)"},
                        ],
                        "recordedVotes": [
                            {
                                "chamber": "House",
                                "congress": 118,
                                "date": "2023-01-05",
                                "rollNumber": 183,
                                "sessionNumber": 1,
                                "url": "https://clerk.house.gov/Votes/2023183",
                            },
                            {
                                "chamber": "House",
                                "congress": 118,
                                "date": "2023-01-06",
                                "rollNumber": 184,
                                "sessionNumber": 1,
                                "url": "https://clerk.house.gov/Votes/2023184",
                            },
                        ],
                    },
                    {
                        "actionCode": "H124",
                        "actionDate": "2023-01-05",
                        "text": "Passed by recorded vote",
                        "type": "Floor",
                        "recordedVotes": [
                            {
                                "chamber": "House",
                                "congress": 118,
                                "date": "2023-01-05",
                                "rollNumber": 183,
                                "sessionNumber": 1,
                                "url": "https://clerk.house.gov/Votes/2023183",
                            },
                            {
                                "chamber": "House",
                                "congress": 118,
                                "date": "2023-01-06",
                                "rollNumber": 184,
                                "sessionNumber": 1,
                                "url": "https://clerk.house.gov/Votes/2023184",
                            },
                        ],
                    },
                ]
            },
            "https://api.congress.gov/v3/bill/118/hr/1?format=json",
        ),
        (
            "https://api.congress.gov/v3/bill/118/hr/1/related-bills?format=json",
            "congress",
            "bills",
            "bill_relatedbills",
            {
                "relatedBills": [
                    {
                        "type": "HR",
                        "number": "2",
                        "congress": 118,
                        "relationshipDetails": [
                            {"type": "Related", "identifiedBy": "Congress.gov"},
                            {"type": "Related", "identifiedBy": "Congress.gov"},
                        ],
                    }
                ]
            },
            "https://api.congress.gov/v3/bill/118/hr/1?format=json",
        ),
        (
            "https://api.congress.gov/v3/committee/house/hsag00/bills?format=json",
            "congress",
            "committees",
            "committee_bills",
            {
                "committee-bills": {
                    "bills": [
                        {
                            "type": "HR",
                            "number": "1",
                            "congress": 118,
                            "relationshipType": "In Committee",
                            "actionDate": "2023-01-05T00:00:00Z",
                            "updateDate": "2023-01-05T00:00:00Z",
                        },
                        {
                            "type": "HR",
                            "number": "1",
                            "congress": 118,
                            "relationshipType": "In Committee",
                            "actionDate": "2023-01-05T00:00:00Z",
                            "updateDate": "2023-01-05T00:00:00Z",
                        }
                    ]
                }
            },
            "https://api.congress.gov/v3/committee/house/hsag00?format=json",
        ),
        (
            "https://api.congress.gov/v3/bill/118/hr/1/committees?format=json",
            "congress",
            "bills",
            "bill_committees",
            {
                "committees": [
                    {"systemCode": "HSAG", "name": "Agriculture Committee"},
                    {"systemCode": "HSAG", "name": "Agriculture Committee"}
                ]
            },
            "https://api.congress.gov/v3/bill/118/hr/1?format=json",
        ),
        (
            "https://api.congress.gov/v3/committee/house/hsag00/reports?format=json",
            "congress",
            "committees",
            "committee_reports",
            {
                "reports": [
                    {
                        "type": "HRPT",
                        "number": 1,
                        "part": 1,
                        "congress": 118,
                        "citation": "H. Rept. 118-1",
                    },
                    {
                        "type": "HRPT",
                        "number": 1,
                        "part": 1,
                        "congress": 118,
                        "citation": "H. Rept. 118-1",
                    },
                ]
            },
            "https://api.congress.gov/v3/committee/house/hsag00?format=json",
        ),
        (
            "https://api.congress.gov/v3/committee/house/hsag00/nominations?format=json",
            "congress",
            "committees",
            "committee_nominations",
            {
                "nominations": [
                    {
                        "number": 123,
                        "partNumber": "01",
                        "congress": 118,
                        "description": "A fixture nomination",
                        "updateDate": "2023-01-06T00:00:00Z",
                    },
                    {
                        "number": 123,
                        "partNumber": "01",
                        "congress": 118,
                        "description": "A fixture nomination (retry)",
                        "updateDate": "2023-01-06T00:00:00Z",
                    },
                ]
            },
            "https://api.congress.gov/v3/committee/house/hsag00?format=json",
        ),
        (
            "https://api.congress.gov/v3/committee/house/hsag00/communications?format=json",
            "congress",
            "committees",
            "committee_communications",
            {
                "houseCommunications": [
                    {
                        "chamber": "House",
                        "communicationType": {
                            "code": "EC",
                            "name": "Executive Communication",
                        },
                        "congress": 118,
                        "number": 3262,
                        "referralDate": "2023-01-06",
                        "updateDate": "2023-01-06T00:00:00Z",
                        "url": "https://api.congress.gov/v3/house-communication/118/ec/3262?format=json",
                    },
                    {
                        "chamber": "House",
                        "communicationType": {
                            "code": "EC",
                            "name": "Executive Communication (retry)",
                        },
                        "congress": 118,
                        "number": 3262,
                        "referralDate": "2023-01-06",
                        "updateDate": "2023-01-06T00:00:00Z",
                        "url": "https://api.congress.gov/v3/house-communication/118/ec/3262?format=json",
                    },
                ]
            },
            "https://api.congress.gov/v3/committee/house/hsag00?format=json",
        ),
        (
            "https://api.congress.gov/v3/bill/118/hr/1/text?format=json",
            "congress",
            "bills",
            "bill_text",
            {
                "textVersions": [
                    {
                        "date": "2023-01-04",
                        "type": "Introduced in House",
                        "formats": [
                            {
                                "type": "Formatted Text",
                                "url": "https://www.congress.gov/118/bills/hr1/BILLS-118hr1ih.htm",
                            }
                        ],
                    }
                ]
            },
            "https://api.congress.gov/v3/bill/118/hr/1?format=json",
        ),
        (
            "https://api.congress.gov/v3/bill/118/hr/1/amendments?format=json",
            "congress",
            "bills",
            "bill_amendments",
            {
                "amendments": [
                    {"type": "SAMDT", "number": 1, "congress": 118},
                    {"type": "SAMDT", "number": 1, "congress": 118},
                ]
            },
            "https://api.congress.gov/v3/bill/118/hr/1?format=json",
        ),
        (
            "https://api.congress.gov/v3/bill/118/hr/1/subjects?format=json",
            "congress",
            "bills",
            "bill_subjects",
            {
                "subjects": {
                    "legislativeSubjects": [
                        {
                            "name": "Government Operations",
                            "updateDate": "2023-01-03T00:00:00Z",
                        },
                        {
                            "name": "Government Operations",
                            "updateDate": "2023-01-04T00:00:00Z",
                        },
                    ]
                }
            },
            "https://api.congress.gov/v3/bill/118/hr/1?format=json",
        ),
        (
            "https://api.congress.gov/v3/bill/118/hr/1/summaries?format=json",
            "congress",
            "bills",
            "bill_summaries",
            {
                "summaries": [
                    {
                        "actionDate": "2023-01-03",
                        "actionDesc": "Introduced in House",
                        "text": "Initial summary",
                        "versionCode": "IH",
                        "updateDate": "2023-01-03T00:00:00Z",
                    },
                    {
                        "actionDate": "2023-01-03",
                        "actionDesc": "Introduced in House",
                        "text": "Corrected summary",
                        "versionCode": "IH",
                        "updateDate": "2023-01-04T00:00:00Z",
                    },
                ]
            },
            "https://api.congress.gov/v3/bill/118/hr/1?format=json",
        ),
        (
            "https://api.congress.gov/v3/committee-report/118/hrpt/1?format=json",
            "congress",
            "committee-reports",
            "committee_report_detail",
            {
                "committeeReports": [
                    {
                        "type": "hrpt",
                        "number": 1,
                        "part": 1,
                        "congress": 118,
                        "sessionNumber": 1,
                        "title": "A committee report for the people",
                        "chamber": "House",
                        "isConferenceReport": False,
                        "citation": "H. Rept. 118-1",
                        "issueDate": "2023-01-04T00:00:00Z",
                        "text": {"count": 2},
                        "updateDate": "2023-01-04T00:00:00Z",
                    }
                ]
            },
            None,
        ),
        (
            "https://api.congress.gov/v3/committee-report/118/hrpt/1/text?format=json&limit=250&offset=0",
            "congress",
            "committee-reports",
            "committee_report_texts",
            {
                "text": [
                    {
                        "date": "2023-01-04",
                        "type": "Final",
                        "formats": [
                            {
                                "type": "Formatted Text",
                                "url": "https://www.congress.gov/118/crpt/hrpt1/CRPT-118hrpt1.htm",
                                "isErrata": "N",
                            },
                            {
                                "type": "PDF",
                                "url": "https://www.congress.gov/118/crpt/hrpt1/CRPT-118hrpt1.pdf",
                                "isErrata": "N",
                            },
                        ],
                    }
                ],
                "pagination": {"count": 1},
            },
            "https://api.congress.gov/v3/committee-report/118/hrpt/1?format=json",
        ),
        (
            "https://api.congress.gov/v3/committee-report/118/hrpt/1/text?format=json&fromDateTime=2023-01-05T00:00:00Z&toDateTime=2023-01-06T00:00:00Z&limit=250&offset=0",
            "congress",
            "committee-reports",
            "committee_report_texts",
            {
                "text": [
                    {
                        "date": "2023-01-05",
                        "type": "Final",
                        "formats": [
                            {
                                "type": "Formatted Text",
                                "url": "https://www.congress.gov/118/crpt/hrpt1/CRPT-118hrpt1-revised.htm",
                                "isErrata": "Y",
                            }
                        ],
                    }
                ],
                "pagination": {"count": 1},
            },
            "https://api.congress.gov/v3/committee-report/118/hrpt/1?format=json",
        ),
        (
            "https://api.congress.gov/v3/amendment/118/samdt/1?format=json",
            "congress",
            "amendments",
            "amendment_detail",
            {
                "amendment": {
                    "type": "SAMDT",
                    "number": 1,
                    "congress": 118,
                    "chamber": "Senate",
                    "purpose": "A fixture amendment",
                    "description": "A fixture amendment for vote-grain tests",
                    "actions": {"count": 1},
                    "amendmentsToAmendment": {"count": 1},
                    "cosponsors": {"count": 1},
                    "sponsors": [
                        {
                            "bioguideId": "A000001",
                            "fullName": "Fixture Member",
                        }
                    ],
                    "textVersions": {"count": 2},
                    "updateDate": "2023-01-05T00:00:00Z",
                }
            },
            None,
        ),
        (
            "https://api.congress.gov/v3/amendment/118/samdt/1/cosponsors?format=json",
            "congress",
            "amendments",
            "amendment_cosponsors",
            {
                "cosponsors": [
                    {
                        "bioguideId": "A000001",
                        "fullName": "Fixture Member",
                    }
                ]
            },
            "https://api.congress.gov/v3/amendment/118/samdt/1?format=json",
        ),
        (
            "https://api.congress.gov/v3/amendment/118/samdt/1/cosponsors?offset=0&format=json",
            "congress",
            "amendments",
            "amendment_cosponsors",
            {
                "cosponsors": [
                    {
                        "bioguideId": "A000001",
                        "fullName": "Fixture Member",
                    }
                ]
            },
            "https://api.congress.gov/v3/amendment/118/samdt/1?format=json",
        ),
        (
            "https://api.congress.gov/v3/amendment/118/samdt/1/text?format=json",
            "congress",
            "amendments",
            "amendment_texts",
            {
                "textVersions": [
                    {
                        "date": "2023-01-05",
                        "type": "Submitted",
                        "formats": [
                            {
                                "type": "HTML",
                                "url": "https://www.congress.gov/118/ams/samdt1/BILLS-118samdt1.htm",
                            },
                            {
                                "type": "PDF",
                                "url": "https://www.congress.gov/118/ams/samdt1/BILLS-118samdt1.pdf",
                            },
                            {
                                # This is a source link carried by the
                                # amendment endpoint, but Congressional Record
                                # extraction is paused for the main-11 release.
                                "type": "HTML",
                                "url": "https://www.congress.gov/118/crec/2023/01/05/169/1/modified/CREC-2023-01-05-pt1-PgS1.htm",
                            },
                        ],
                    },
                    {
                        "date": "2023-01-06",
                        "type": "Engrossed",
                        "formats": [
                            {
                                "type": "HTML",
                                "url": "https://www.congress.gov/118/ams/samdt1/BILLS-118samdt1es.htm",
                            }
                        ],
                    },
                ]
            },
            "https://api.congress.gov/v3/amendment/118/samdt/1?format=json",
        ),
        (
            "https://api.congress.gov/v3/amendment/118/samdt/1/actions?format=json",
            "congress",
            "amendments",
            "amendment_actions",
            {
                "actions": [
                    {
                        "actionCode": "S123",
                        "actionDate": "2023-01-05",
                        "text": "Amendment recorded votes",
                        "type": "Floor",
                        "recordedVotes": [
                            {
                                "chamber": "Senate",
                                "congress": 118,
                                "date": "2023-01-05",
                                "rollNumber": 354,
                                "sessionNumber": 1,
                                "url": "https://www.senate.gov/legislative/LIS/roll_call_votes/vote1181/vote_118_1_00354.htm",
                            },
                            {
                                "chamber": "Senate",
                                "congress": 118,
                                "date": "2023-01-06",
                                "rollNumber": 355,
                                "sessionNumber": 1,
                                "url": "https://www.senate.gov/legislative/LIS/roll_call_votes/vote1181/vote_118_1_00355.htm",
                            },
                        ],
                    },
                    {
                        "actionCode": "S123",
                        "actionDate": "2023-01-05",
                        "text": "Amendment recorded votes",
                        "type": "Floor",
                        "recordedVotes": [
                            {
                                "chamber": "Senate",
                                "congress": 118,
                                "date": "2023-01-05",
                                "rollNumber": 354,
                                "sessionNumber": 1,
                                "url": "https://www.senate.gov/legislative/LIS/roll_call_votes/vote1181/vote_118_1_00354.htm",
                            },
                            {
                                "chamber": "Senate",
                                "congress": 118,
                                "date": "2023-01-06",
                                "rollNumber": 355,
                                "sessionNumber": 1,
                                "url": "https://www.senate.gov/legislative/LIS/roll_call_votes/vote1181/vote_118_1_00355.htm",
                            },
                        ],
                    },
                    {
                        "actionCode": "S124",
                        "actionDate": "2023-01-05",
                        "text": "Amendment recorded votes",
                        "type": "Floor",
                        "recordedVotes": [
                            {
                                "chamber": "Senate",
                                "congress": 118,
                                "date": "2023-01-05",
                                "rollNumber": 354,
                                "sessionNumber": 1,
                                "url": "https://www.senate.gov/legislative/LIS/roll_call_votes/vote1181/vote_118_1_00354.htm",
                            },
                            {
                                "chamber": "Senate",
                                "congress": 118,
                                "date": "2023-01-06",
                                "rollNumber": 355,
                                "sessionNumber": 1,
                                "url": "https://www.senate.gov/legislative/LIS/roll_call_votes/vote1181/vote_118_1_00355.htm",
                            },
                        ],
                    },
                ]
            },
            "https://api.congress.gov/v3/amendment/118/samdt/1?format=json",
        ),
        (
            "https://api.congress.gov/v3/amendment/118/samdt/1/amendments?format=json",
            "congress",
            "amendments",
            "amendment_amendments_to_amendment",
            {
                "amendments": [
                    {
                        "type": "SAMDT",
                        "number": "2",
                        "congress": 118,
                        "updateDate": "2023-01-06T00:00:00Z",
                        "url": "https://api.congress.gov/v3/amendment/118/samdt/2?format=json",
                    },
                    {
                        "type": "SAMDT",
                        "number": "2",
                        "congress": 118,
                        "updateDate": "2023-01-06T00:00:00Z",
                        "url": "https://api.congress.gov/v3/amendment/118/samdt/2?format=json",
                    },
                ]
            },
            "https://api.congress.gov/v3/amendment/118/samdt/1?format=json",
        ),
        (
            "https://api.congress.gov/v3/nomination/118/123-01?format=json",
            "congress",
            "nominations",
            "nomination_detail",
            {
                "nomination": {
                    "number": "123",
                    "partNumber": "01",
                    "congress": 118,
                    "description": "A fixture nomination",
                    "receivedDate": "2023-01-05",
                    "authorityDate": "2023-01-04",
                    "actions": {"count": 1},
                    "committees": {"count": 1},
                    "hearings": {"count": 1},
                    "nominees": {"count": 2},
                    "updateDate": "2023-01-06T00:00:00Z",
                }
            },
            None,
        ),
        (
            "https://api.congress.gov/v3/nomination/118/123-01/nominees?format=json",
            "congress",
            "nominations",
            "nomination_nominee_detail",
            {
                "nominees": [
                    {
                        "ordinal": 1,
                        "firstName": "Alex",
                        "lastName": "Alpha",
                        "state": "CA",
                    },
                    {
                        "ordinal": 1,
                        "firstName": "Casey",
                        "lastName": "Beta",
                        "state": "NY",
                    },
                ]
            },
            "https://api.congress.gov/v3/nomination/118/123-01?format=json",
        ),
        (
            "https://api.congress.gov/v3/nomination/118/123-01/actions?format=json",
            "congress",
            "nominations",
            "nomination_actions",
            {
                "actions": [
                    {
                        "actionCode": "SENATE",
                        "actionDate": "2023-01-06",
                        "text": "Referred to committee",
                        "type": "Committee",
                        "committees": [
                            {"systemCode": "HSAG", "name": "Agriculture Committee"},
                            {"systemCode": "HSAG", "name": "Agriculture Committee (corrected)"},
                        ],
                    },
                    {
                        "actionCode": "SENATE",
                        "actionDate": "2023-01-06",
                        "text": "Referred to committee",
                        "type": "Committee",
                        "committees": [
                            {"systemCode": "HSAG", "name": "Agriculture Committee"},
                            {"systemCode": "HSAG", "name": "Agriculture Committee (corrected)"},
                        ],
                    },
                ]
            },
            "https://api.congress.gov/v3/nomination/118/123-01?format=json",
        ),
        (
            "https://api.congress.gov/v3/nomination/118/123-01/hearings?format=json",
            "congress",
            "nominations",
            "nomination_hearings",
            {
                "hearings": [
                    {
                        "chamber": "House",
                        "jacketNumber": 10,
                        "partNumber": "1",
                        "number": "10",
                        "date": "2023-01-06",
                        "citation": "H. Hrg. 118-10",
                    },
                    {
                        "chamber": "House",
                        "jacketNumber": 10,
                        "partNumber": "1",
                        "number": "10",
                        "date": "2023-01-06",
                        "citation": "H. Hrg. 118-10",
                    },
                ]
            },
            "https://api.congress.gov/v3/nomination/118/123-01?format=json",
        ),
        (
            "https://api.congress.gov/v3/nomination/118/123-01/committees?format=json",
            "congress",
            "nominations",
            "nomination_committees",
            {
                "committees": [
                    {
                        "systemCode": "HSAG",
                        "name": "Agriculture Committee",
                        "type": "Standing",
                        "activities": [
                            {"name": "Reported", "date": "2023-01-07"},
                            {"name": "Reported", "date": "2023-01-07"},
                        ],
                    }
                ]
            },
            "https://api.congress.gov/v3/nomination/118/123-01?format=json",
        ),
        (
            "https://api.congress.gov/v3/treaty/118/13?format=json",
            "congress",
            "treaties",
            "treaty_detail",
            {
                "treaty": [
                    {
                        "number": 13,
                        "congressReceived": 118,
                        "congressConsidered": 118,
                        "topic": "A fixture treaty",
                        "parts": {"count": 1},
                        "actions": {"count": 1},
                        "updateDate": "2023-01-08T00:00:00Z",
                    }
                ]
            },
            None,
        ),
        (
            "https://api.congress.gov/v3/treaty/118/13/committees?format=json",
            "congress",
            "treaties",
            "treaty_committees",
            {
                "treatyCommittees": [
                    {
                        "systemCode": "HSAG",
                        "name": "Agriculture Committee",
                        "type": "Standing",
                        "chamber": "House",
                        "activities": [
                            {"name": "Considered", "date": "2023-01-09"},
                            {"name": "Considered", "date": "2023-01-09"},
                        ],
                    },
                    {
                        "systemCode": "HSAG",
                        "name": "Agriculture Committee",
                        "type": "Standing",
                        "chamber": "House",
                        "activities": [
                            {"name": "Considered", "date": "2023-01-09"},
                        ],
                    },
                ]
            },
            "https://api.congress.gov/v3/treaty/118/13?format=json",
        ),
        (
            "https://api.congress.gov/v3/treaty/118/13/actions?format=json",
            "congress",
            "treaties",
            "treaty_actions",
            {
                "actions": [
                    {
                        "actionCode": "SENATE",
                        "actionDate": "2023-01-10",
                        "text": "Referred to committee",
                        "type": "Committee",
                        "committee": {
                            "systemCode": "HSAG",
                            "name": "Agriculture Committee",
                        },
                    },
                    {
                        "actionCode": "SENATE",
                        "actionDate": "2023-01-10",
                        "text": "Referred to committee",
                        "type": "Committee",
                        "committee": {
                            "systemCode": "HSAG",
                            "name": "Agriculture Committee",
                        },
                    },
                ]
            },
            "https://api.congress.gov/v3/treaty/118/13?format=json",
        ),
        (
            "https://api.congress.gov/v3/committee-print/118/house/47777?format=json",
            "congress",
            "committee-prints",
            "committee_print_detail",
            {
                "committeePrint": [
                    {
                        "chamber": "House",
                        "congress": 118,
                        "jacketNumber": 47777,
                        "text": {
                            "count": 1,
                            "url": "https://api.congress.gov/v3/committee-print/118/house/47777/text?format=json",
                        },
                        "title": "A fixture committee print",
                        "updateDate": "2023-01-08T00:00:00Z",
                    }
                ]
            },
            None,
        ),
        (
            "https://api.congress.gov/v3/committee-print/118/house/47777/text?format=json",
            "congress",
            "committee-prints",
            "committee_print_texts",
            {
                "text": [
                    {
                        "date": "2023-01-08",
                        "formats": [
                            {
                                "type": "PDF",
                                "url": "https://www.congress.gov/118/committee-print/47777/CPRT-118HPRT47777.pdf",
                            },
                            {
                                "type": "Generated HTML",
                                "url": "https://www.congress.gov/118/committee-print/47777/CPRT-118HPRT47777.htm",
                            },
                            {
                                "type": "Generated HTML",
                                "url": "https://www.congress.gov/118/committee-print/47777/CPRT-118HPRT47777-revised.htm",
                            },
                        ],
                    },
                    {
                        "type": "Formatted Text",
                        "url": "https://www.congress.gov/118/cprt/HPRT47777/CPRT-118HPRT47777-direct.htm",
                    },
                ],
                "pagination": {"count": 1},
            },
            "https://api.congress.gov/v3/committee-print/118/house/47777?format=json",
        ),
        (
            "https://api.govinfo.gov/packages/BILLS-118hr1ih/summary?api_key=ci",
            "govinfo",
            "govinfo-bills",
            "govinfo_bill_package_detail",
            {
                "packageId": "BILLS-118hr1ih",
                "billType": "HR",
                "billNumber": 1,
                "congress": 118,
                "billVersion": "ih",
                "title": "A bill for the people",
                "originChamber": "HOUSE",
                "currentChamber": "HOUSE",
                "dateIssued": "2023-01-03",
                "lastModified": "2023-01-04T00:00:00Z",
                "collectionCode": "BILLS",
            },
            None,
        ),
        (
            "https://api.govinfo.gov/packages/BILLS-118hr2ih/summary?api_key=ci",
            "govinfo",
            "govinfo-bills",
            "govinfo_bill_package_detail",
            {
                "packageId": "BILLS-118hr2ih",
                "billType": "HR",
                "billNumber": 2,
                "congress": 118,
                "billVersion": "ih",
                "title": "A related bill",
                "originChamber": "HOUSE",
                "currentChamber": "HOUSE",
                "dateIssued": "2023-01-05",
                "lastModified": "2023-01-05T00:00:00Z",
                "collectionCode": "BILLS",
            },
            None,
        ),
        (
            "https://api.govinfo.gov/packages/CRPT-118hrpt1/summary?api_key=ci",
            "govinfo",
            "govinfo-committee-reports",
            "govinfo_committee_report_package_detail",
            {
                "packageId": "CRPT-118hrpt1",
                "docClass": "hrpt",
                "documentNumber": "1",
                "congress": 118,
                "session": 1,
                "title": "A committee report for the people",
                "chamber": "HOUSE",
                "dateIssued": "2023-01-04",
                "lastModified": "2023-01-04T00:00:00Z",
                "collectionCode": "CRPT",
            },
            None,
        ),
    ]


def main() -> int:
    if os.getenv("BICAM_CI_FIXTURES") != "1":
        raise SystemExit("refusing to load fixtures without BICAM_CI_FIXTURES=1")

    database_url = os.environ["DATABASE_URL"]
    with psycopg.connect(database_url) as conn:
        for fixture_index, (
            endpoint_url,
            source,
            category,
            node_type,
            response,
            parent_endpoint_url,
        ) in enumerate(_fixture_rows()):
            raw_id = uuid.uuid5(uuid.NAMESPACE_URL, endpoint_url)
            parent_id = (
                uuid.uuid5(uuid.NAMESPACE_URL, parent_endpoint_url)
                if parent_endpoint_url
                else None
            )
            response_json = json.dumps(response, sort_keys=True)
            response_hash = hashlib.md5(response_json.encode(), usedforsecurity=False).hexdigest()
            conn.execute(
                """
                INSERT INTO raw_responses (
                    id, endpoint_url, source, category, node_type, parent_id, response,
                    response_hash, fetched_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb, %s,
                          NOW() + (%s * interval '1 millisecond'))
                ON CONFLICT (endpoint_url) DO UPDATE SET
                    source = EXCLUDED.source,
                    category = EXCLUDED.category,
                    node_type = EXCLUDED.node_type,
                    parent_id = EXCLUDED.parent_id,
                    response = EXCLUDED.response,
                    response_hash = EXCLUDED.response_hash,
                    fetched_at = EXCLUDED.fetched_at
                """,
                (
                    raw_id,
                    endpoint_url,
                    source,
                    category,
                    node_type,
                    parent_id,
                    response_json,
                    response_hash,
                    fixture_index,
                ),
            )
            conn.execute(
                """
                INSERT INTO raw_response_versions (
                    id, raw_response_id, endpoint_url, source, category, node_type,
                    response, response_hash, parent_id, first_seen_at, last_seen_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s,
                          NOW() + (%s * interval '1 millisecond'),
                          NOW() + (%s * interval '1 millisecond'))
                ON CONFLICT (raw_response_id, response_hash) DO NOTHING
                """,
                (
                    uuid.uuid5(uuid.NAMESPACE_URL, f"version:{endpoint_url}:{response_hash}"),
                    raw_id,
                    endpoint_url,
                    source,
                    category,
                    node_type,
                    response_json,
                    response_hash,
                    parent_id,
                    fixture_index,
                    fixture_index,
                ),
            )
        # Seed one successful out-of-scope extraction so the public amendment
        # mart test proves the Congressional Record link is retained as source
        # metadata but its prose is never exposed as amendment text.
        record_url = (
            "https://www.congress.gov/118/crec/2023/01/05/169/1/modified/"
            "CREC-2023-01-05-pt1-PgS1.htm"
        )
        record_content = "Congressional Record fixture prose"
        record_hash = hashlib.sha256(record_content.encode()).hexdigest()
        conn.execute(
            """
            INSERT INTO html_text_versions (
                id, url, response_hash, content_type, http_status, fetched_at,
                extraction_status, text_content, error
            ) VALUES (%s, %s, %s, %s, %s, NOW(), 'success', %s, NULL)
            ON CONFLICT (url, response_hash) DO NOTHING
            """,
            (
                uuid.uuid5(uuid.NAMESPACE_URL, f"html:{record_url}:{record_hash}"),
                record_url,
                record_hash,
                "text/html; charset=utf-8",
                200,
                record_content,
            ),
        )
        conn.commit()
    print(f"loaded {len(_fixture_rows())} CI fixture payloads")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

bills: {lower(type)}{number}-{congress}
amendments: {lower(type)}{number}-{congress}
members: bioguide_id (aka bioguideid)
committees: committee_code (aka systemcode)
committeereports: {lower(type)}{number}-{optional "part" then -}{congress}
committeeprints: {lower(type)}{number}-{congress}
nominations: PN{number}-{part number or 00}-{congress}
treaties: td{congressreceived}-{number}
committemeetings: meeting_id (aka eventid)
hearings: {lower(first letter of chamber), "j" if chamber is "nochamber"}hrg-{congress}
congresses: number


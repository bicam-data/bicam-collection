NOTE: top level key is `subjects`, but then there may be many subjects within `legislativeSubjects`. There is then one `policyArea`.

```json
{
  "subjects" : {
  "legislativeSubjects": [
    {
      "name": "Congressional oversight",
      "updateDate": "2022-02-18T16:38:41Z"
    },
    {
      "name": "U.S. Postal Service",
      "updateDate": "2023-02-18T16:38:41Z"
    }
  ],
  "policyArea": {
    "name": "Government Operations and Politics"
  }
},
"pagination": {
    "count": 17
  },
  "request": {
    "billNumber": "3076",
    "billType": "hr",
    "billUrl": "https://api.congress.gov/v3/bill/117/hr/3076?format=json",
    "congress": "117",
    "contentType": "application/json",
    "format": "json"
  }
}```

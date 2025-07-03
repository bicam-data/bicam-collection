from collections.abc import Mapping
import polars as pl

# Identification for actions rows
RAW_ID_FIELD = "actionId"
ID_ALIAS = "action_id"

FIELD_MAP: Mapping[str, str] = {
    RAW_ID_FIELD: ID_ALIAS,
    "billId": "bill_id",
    "actionCode": "action_code",
    "actionType": "action_type",
    "actionDate": "action_date",
    "text": "text",
}

KEY_COLUMNS = [ID_ALIAS]

# Nested recorded votes inside each action
NESTED_ARRAYS = {
    "recorded_votes": {
        "json_path": "$.recordedVotes[*]",
        "field_map": {
            "chamber": "chamber",
            "congress": "congress",
            "date": "vote_date",
            "rollNumber": "roll_number",
            "sessionNumber": "session",
            "url": "url",
        },
        "key_columns": [ID_ALIAS, "roll_number"],
    }
}


def custom_transforms(df: pl.LazyFrame) -> pl.LazyFrame:
    return df 
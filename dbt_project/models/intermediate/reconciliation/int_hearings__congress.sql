{{ config(
    tags=["hearings", "reconciliation", "intermediate"],
    indexes=[
        {"columns": ["hearing_id"], "unique": true},
        {"columns": ["congress", "hearing_jacketnumber", "chamber"]}
    ]
) }}

select *
from {{ ref('stg_congress__hearing_detail') }}

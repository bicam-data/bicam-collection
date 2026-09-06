{{ config(
    tags=["bill_reconciliation", "reconciliation", "intermediate"],
    indexes=[
        {"columns": ["canonical_bill_id", "normalized_title"], "unique": true}
    ]
) }}

with variants as (
    select
        canonical_bill_id,
        title,
        normalized_title,
        'selected_primary'::text as title_source,
        null::text as title_type,
        null::text as title_type_code,
        source_updated_at as updated_at
    from {{ ref('int_bills__congress') }}
    where normalized_title is not null

    union all

    select
        titles.bill_id as canonical_bill_id,
        titles.title,
        {{ normalize_match_text('titles.title') }} as normalized_title,
        'congress_title_variant'::text as title_source,
        titles.title_type,
        titles.title_type_code,
        titles.updated_at
    from {{ ref('stg_congress__bill_titles') }} as titles
    where {{ normalize_match_text('titles.title') }} is not null
),
ranked as (
    select
        *,
        row_number() over (
            partition by canonical_bill_id, normalized_title
            order by
                (title_source = 'selected_primary') desc,
                updated_at desc nulls last,
                title_type_code nulls last,
                title
        ) as variant_rank
    from variants
)

select
    canonical_bill_id,
    title,
    normalized_title,
    title_source,
    title_type,
    title_type_code,
    updated_at
from ranked
where variant_rank = 1

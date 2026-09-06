{{ config(tags=["nominations", "staging"]) }}

with detail as (
    select
        response -> 'nomination' as nomination_json
    from {{ ref('int_raw_responses__current') }}
    where source = 'congress'
      and category = 'nominations'
      and node_type = 'nomination_detail'
),
unnested as (
    select
        'pn'
        || {{ json_text('nomination_json', 'number') }}
        || '-'
        || {{ json_int('nomination_json', 'congress') }}::text as nomination_id,
        'pn'
        || {{ json_text('nomination_json', 'number') }}
        || '-'
        || coalesce({{ json_text('nomination_json', 'partNumber') }}, '00')
        || '-'
        || {{ json_int('nomination_json', 'congress') }}::text as nomination_part_id,
        nominee.value as nominee_json
    from detail
    cross join lateral jsonb_array_elements(
        case
            when jsonb_typeof(nomination_json #> {{ bicam_json_path_literal("nominees") }}) = 'array'
                then nomination_json #> {{ bicam_json_path_literal("nominees") }}
            else '[]'::jsonb
        end
    ) as nominee(value)
)

select
    nomination_id,
    nomination_part_id,
    {{ json_int('nominee_json', 'ordinal') }} as position_id,
    {{ json_text('nominee_json', 'positionTitle') }} as position_title,
    {{ json_text('nominee_json', 'organization') }} as organization,
    {{ json_text('nominee_json', 'division') }} as division,
    {{ json_text('nominee_json', 'introText') }} as intro_text,
    coalesce({{ json_int('nominee_json', 'nomineeCount') }}, 0) as nominee_count
from unnested

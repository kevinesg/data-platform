{{ config(
    materialized='view'
) }}

-- Match rows are immutable stage outputs. Keeping this compatibility union as a
-- view avoids materializing the full cross-stage relation in one query.
with combined_raw as (
    select * from {{ ref('int_wremotely__country_eligibility_atomic_matches') }}
    union all

    select * from {{ ref('int_wremotely__country_eligibility_country_text_matches') }}
    union all

    select * from {{ ref('int_wremotely__country_eligibility_country_group_text_matches') }}
    union all

    select * from {{ ref('int_wremotely__country_eligibility_subdivision_text_matches') }}
)

select
    match_id
    , evidence_id
    , source_landing_run_id
    , candidate_id
    , evidence_direction
    , matched_country_code
    , matched_country_group_code
    , match_source
from combined_raw

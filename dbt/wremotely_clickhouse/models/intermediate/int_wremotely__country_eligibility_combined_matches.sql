{{ config(
    materialized='table',
    on_schema_change='append_new_columns',
    order_by="ifNull(match_id, '')",
    query_settings={
        'max_threads': 1,
        'max_bytes_before_external_sort': 268435456,
        'max_bytes_before_external_group_by': 268435456,
        'join_algorithm': 'grace_hash',
        'max_memory_usage': 8589934592
    }
) }}

{% set incremental_watermark_ready = is_incremental()
    and relation_has_columns(this, ['source_landing_run_id']) %}

-- Match rows are immutable stage outputs. Persisting this compatibility union
-- avoids re-expanding all four match relations for every downstream test/query.
with combined_raw as (
    select * from {{ ref('int_wremotely__country_eligibility_atomic_matches') }}
    union all

    select * from {{ ref('int_wremotely__country_eligibility_country_text_matches') }}
    union all

    select * from {{ ref('int_wremotely__country_eligibility_country_group_text_matches') }}
    union all

    select * from {{ ref('int_wremotely__country_eligibility_subdivision_text_matches') }}
),

changed_matches as (
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
    {% if incremental_watermark_ready %}
    where source_landing_run_id >= (
        select coalesce(max(source_landing_run_id), '')
        from {{ this }}
    )
    {% endif %}
)

select
    *
from changed_matches

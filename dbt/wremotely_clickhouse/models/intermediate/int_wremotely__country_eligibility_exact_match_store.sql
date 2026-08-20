{{ config(
    materialized='table',
    on_schema_change='append_new_columns',
    order_by='assumeNotNull(match_id)',
    query_settings={
        'max_threads': 1,
        'max_bytes_before_external_group_by': 67108864,
        'max_bytes_before_external_sort': 67108864,
        'max_memory_usage': 1073741824
    }
) }}

{% set incremental_watermark_ready = is_incremental()
    and relation_has_columns(this, ['source_landing_run_id']) %}

with exact_matches as (
    select
        match_id
        , evidence_id
        , source_landing_run_id
        , candidate_id
        , evidence_direction
        , matched_country_code
        , matched_country_group_code
        , match_source
        , match_status
    from {{ ref('int_wremotely__country_eligibility_exact_matches') }}
    {% if incremental_watermark_ready %}
    where source_landing_run_id > (
        select coalesce(max(source_landing_run_id), '')
        from {{ this }}
    )
    {% endif %}
)

select *
from exact_matches

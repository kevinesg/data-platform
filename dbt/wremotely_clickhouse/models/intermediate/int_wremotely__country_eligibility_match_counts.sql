{{ config(
    materialized='table',
    on_schema_change='append_new_columns',
    order_by="ifNull(evidence_id, '')",
    query_settings={
        'max_threads': 1,
        'max_bytes_before_external_group_by': 268435456,
        'max_memory_usage': 1073741824
    }
) }}

{% set incremental_watermark_ready = is_incremental()
    and relation_has_columns(this, ['source_landing_run_id']) %}

with country_matches as (
    select
        evidence_id
        , source_landing_run_id
        , matched_country_code
    from {{ ref('int_wremotely__country_eligibility_combined_matches') }}
    {% if incremental_watermark_ready %}
    where source_landing_run_id > (
        select coalesce(max(source_landing_run_id), '')
        from {{ this }}
    )
    {% endif %}
)

select
    evidence_id
    , max(source_landing_run_id) as source_landing_run_id
    , uniqExactIf(
        matched_country_code
        , notEmpty(ifNull(matched_country_code, ''))
    ) as matched_country_count
from country_matches
group by evidence_id

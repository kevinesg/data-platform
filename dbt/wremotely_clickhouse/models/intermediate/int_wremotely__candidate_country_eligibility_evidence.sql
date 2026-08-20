{{ config(
    materialized='table',
    on_schema_change='append_new_columns',
    order_by="(ifNull(candidate_id, ''), ifNull(evidence_id, ''))",
    query_settings={
        'max_threads': 1,
        'max_bytes_before_external_group_by': 268435456,
        'max_bytes_before_external_sort': 268435456,
        'join_algorithm': 'grace_hash',
        'max_memory_usage': 8589934592
    }
) }}

{% set incremental_watermark_ready = is_incremental()
    and relation_has_columns(this, ['source_landing_run_id']) %}

with inputs as (
    select
        candidate_id
        , source_landing_run_id
        , evidence_id
        , evidence_direction
    from {{ ref('int_wremotely__country_eligibility_inputs') }}
    {% if incremental_watermark_ready %}
    where source_landing_run_id > (
        select coalesce(max(source_landing_run_id), '')
        from {{ this }}
    )
    {% endif %}
),

exact_matches as (
    select
        evidence_id
        , match_id
        , match_source
        , matched_country_code
        , matched_country_group_code
        , match_status
    from {{ ref('int_wremotely__country_eligibility_exact_match_store') }}
)

select
    concat(
        inputs.source_landing_run_id
        , '|'
        , inputs.evidence_id
        , '|', ifNull(matches.match_id, '')
        , '|', ifNull(matches.matched_country_code, '')
        , '|', ifNull(matches.matched_country_group_code, '')
        , '|', ifNull(matches.match_status, '')
    ) as evidence_row_id
    , inputs.candidate_id
    , inputs.source_landing_run_id
    , inputs.evidence_id
    , inputs.evidence_direction
    , matches.match_source
    , matches.matched_country_code
    , matches.matched_country_group_code
    , matches.match_status
from inputs
left join exact_matches as matches
    on inputs.evidence_id = matches.evidence_id

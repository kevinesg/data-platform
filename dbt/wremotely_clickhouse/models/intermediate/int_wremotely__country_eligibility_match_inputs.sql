{{ config(
    materialized='table',
    on_schema_change='append_new_columns',
    order_by="(ifNull(source_landing_run_id, ''), ifNull(evidence_id, ''))",
    query_settings={
        'max_threads': 2,
        'max_bytes_before_external_sort': 268435456,
        'max_memory_usage': 8589934592
    }
) }}

{% set incremental_watermark_ready = is_incremental()
    and relation_has_columns(this, ['source_landing_run_id']) %}

select
    evidence_id
    , source_landing_run_id
    , candidate_id
    , country_field_role
    , json_path
    , evidence_direction
    , country_match_mode
    , normalized_raw_value
    , source_platform_guess
    , is_restricting_location_evidence
from {{ ref('int_wremotely__country_eligibility_inputs') }}
where evidence_direction in ('INCLUDED', 'EXCLUDED')
    and nullIf(normalized_raw_value, '') is not null
    {% if incremental_watermark_ready %}
    and source_landing_run_id > (
        select coalesce(max(source_landing_run_id), '')
        from {{ this }}
    )
    {% endif %}

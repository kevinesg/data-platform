{{ config(
    materialized='table',
    on_schema_change='append_new_columns',
    order_by="(ifNull(candidate_id, ''))",
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

with changed_candidates as (
    select distinct candidate_id
    from {{ ref('int_wremotely__country_eligibility_inputs') }}
    {% if incremental_watermark_ready %}
    where source_landing_run_id > (
        select coalesce(max(source_landing_run_id), '')
        from {{ this }}
    )
    {% endif %}
),

evidence as (
    select
        candidate_evidence.candidate_id
        , candidate_evidence.source_landing_run_id
        , candidate_evidence.evidence_id
        , candidate_evidence.evidence_direction
        , candidate_evidence.matched_country_code
        , candidate_evidence.matched_country_group_code
        , candidate_evidence.match_status
    from {{ ref('int_wremotely__candidate_country_eligibility_evidence') }} as candidate_evidence
    any inner join changed_candidates as changed
        on candidate_evidence.candidate_id = changed.candidate_id
),

group_memberships as (
    select
        country_group_code
        , country_code
    from {{ ref('wremotely__country_group_memberships') }}
),

country_direction_rows as (
    select
        candidate_id
        , evidence_direction
        , matched_country_code as country_code
    from evidence
    where match_status = 'MATCHED'
        and notEmpty(ifNull(matched_country_code, ''))

    union all

    select
        e.candidate_id
        , e.evidence_direction
        , gm.country_code
    from evidence as e
    inner join group_memberships as gm
        on e.matched_country_group_code = gm.country_group_code
    where e.match_status = 'MATCHED'
        and notEmpty(ifNull(e.matched_country_group_code, ''))
),

country_rollup as (
    select
        candidate_id
        , arraySort(groupUniqArray(country_code)) as country_codes
        , arraySort(groupUniqArrayIf(country_code, evidence_direction = 'INCLUDED'))
            as included_country_codes
        , arraySort(groupUniqArrayIf(country_code, evidence_direction = 'EXCLUDED'))
            as excluded_country_codes
    from country_direction_rows
    group by candidate_id
),

group_rollup as (
    select
        candidate_id
        , arraySort(
            groupUniqArrayIf(matched_country_group_code, evidence_direction = 'INCLUDED')
        ) as included_country_group_codes
        , arraySort(
            groupUniqArrayIf(matched_country_group_code, evidence_direction = 'EXCLUDED')
        ) as excluded_country_group_codes
    from evidence
    where match_status = 'MATCHED'
        and notEmpty(ifNull(matched_country_group_code, ''))
    group by candidate_id
),

evidence_rollup as (
    select
        evidence_rows.candidate_id
        , max(evidence_rows.source_landing_run_id) as source_landing_run_id
        , max(evidence_rows.evidence_direction = 'GLOBAL') as has_global_evidence
        , max(evidence_rows.evidence_direction = 'UNKNOWN') as has_unknown_evidence
        , uniqExact(evidence_rows.evidence_id) as country_eligibility_evidence_count
        , uniqExactIf(
            evidence_rows.evidence_id
            , evidence_rows.match_status = 'MATCHED'
                and notEmpty(ifNull(evidence_rows.matched_country_code, ''))
        ) as matched_country_evidence_count
        , uniqExactIf(
            evidence_rows.evidence_id
            , evidence_rows.match_status = 'MATCHED'
                and notEmpty(ifNull(evidence_rows.matched_country_group_code, ''))
        ) as matched_country_group_evidence_count
    from (select * from evidence) as evidence_rows
    group by evidence_rows.candidate_id
),

combined as (
    select
        er.candidate_id as candidate_id
        , er.source_landing_run_id
        , case
            when length(ifNull(cr.included_country_codes, [])) > 0 then 'SPECIFIC'
            when er.has_global_evidence
                and length(ifNull(cr.excluded_country_codes, [])) > 0
                then 'GLOBAL_EXCEPT'
            when er.has_global_evidence then 'GLOBAL'
            else 'UNKNOWN'
        end as validated_country_eligibility_scope
        , arrayFilter(
            country_code -> not has(ifNull(cr.excluded_country_codes, []), country_code)
            , ifNull(cr.included_country_codes, [])
        ) as eligible_country_codes
        , ifNull(cr.excluded_country_codes, []) as excluded_country_codes
        , ifNull(gr.included_country_group_codes, []) as included_country_group_codes
        , ifNull(gr.excluded_country_group_codes, []) as excluded_country_group_codes
        , er.has_global_evidence
        , er.has_unknown_evidence
        , er.country_eligibility_evidence_count
        , er.matched_country_evidence_count
        , er.matched_country_group_evidence_count
        , now64(3) as dbt_updated_at
    from evidence_rollup as er
    left join country_rollup as cr
        on er.candidate_id = cr.candidate_id
    left join group_rollup as gr
        on er.candidate_id = gr.candidate_id
)

select *
from combined

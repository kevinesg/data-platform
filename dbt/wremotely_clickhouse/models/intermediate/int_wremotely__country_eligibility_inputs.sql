{{ config(
    materialized='table',
    on_schema_change='append_new_columns',
    order_by="(ifNull(normalized_raw_value, ''), ifNull(country_match_mode, ''), ifNull(evidence_id, ''))"
) }}

{% set incremental_watermark_ready = is_incremental()
    and relation_has_columns(this, ['source_landing_run_id']) %}

with raw_evidence as (
    select
        raw.*
        , latest.latest_remote_scope as classification_remote_scope
        , latest.latest_country_eligibility_scope as classification_country_scope
    from {{ ref('stg_wremotely__country_eligibility_extractions') }} as raw
    inner join {{ ref('int_wremotely__latest_classifications') }} as latest
        on raw.candidate_id = latest.candidate_id
        and raw.stage_run_id = latest.latest_classification_stage_run_id
        and raw.classification_run_id = latest.latest_classification_run_id
    {% if incremental_watermark_ready %}
        and raw.landing_run_id > (
            select coalesce(max(source_landing_run_id), '')
            from {{ this }}
        )
    {% endif %}
),

prepared_roles as (
    select
        *
        , nullIf(
            trim(replaceRegexpAll(lowerUTF8(ifNull(raw_value, '')), '[^[:alnum:]]+', ' '))
            , ''
        ) as normalized_raw_value
        , nullIf(
            trim(replaceRegexpAll(ifNull(raw_value, ''), '[^A-Za-z0-9]+', ' '))
            , ''
        ) as case_sensitive_search_text
        , replaceRegexpAll(
            lowerUTF8(ifNull(json_path, ''))
            , '(\\.address)?\\.(addresscountry|addressregion|addresslocality|name)$'
            , ''
        ) as location_object_path
        , country_field_role in (
            'JOB_LOCATION'
            , 'PLATFORM_JOB_LOCATION'
            , 'BAMBOOHR_CAREERS_LIST'
            , 'BREEZY_META'
            , 'GREENHOUSE_REMIX'
            , 'JAZZHR_VISIBLE_HTML'
            , 'JOBVITE_PRELOADED_DATA'
            , 'NEXTJS'
            , 'PERSONIO_VISIBLE_HTML'
            , 'SMARTRECRUITERS_MICRODATA'
            , 'WORKABLE_WIDGET'
        ) as is_location_evidence_role
        , (
            country_field_role = 'PLATFORM_JOB_LOCATION'
            and lowerUTF8(ifNull(source_platform_guess, '')) in (
                'ashby'
                , 'bamboohr'
                , 'breezy'
                , 'greenhouse'
                , 'jazzhr'
                , 'jobvite'
                , 'lever'
                , 'personio'
                , 'rippling'
                , 'smartrecruiters'
                , 'workable'
            )
        ) or (
            country_field_role = country_field_source_system
            and country_field_role in (
                'BAMBOOHR_CAREERS_LIST'
                , 'BREEZY_META'
                , 'GREENHOUSE_REMIX'
                , 'JOBVITE_PRELOADED_DATA'
                , 'NEXTJS'
                , 'PERSONIO_VISIBLE_HTML'
                , 'SMARTRECRUITERS_MICRODATA'
                , 'WORKABLE_WIDGET'
            )
        ) as is_reviewed_platform_location_role
    from raw_evidence
),

global_location_aliases as (
    select distinct
        lowerUTF8(source_platform_guess) as source_platform_guess
        , nullIf(
            trim(replaceRegexpAll(lowerUTF8(location_alias), '[^[:alnum:]]+', ' '))
            , ''
        ) as alias_search_text
    from {{ ref('wremotely__global_location_aliases') }}
    where nullIf(trim(source_platform_guess), '') is not null
        and nullIf(trim(location_alias), '') is not null
),

prepared_inputs as (
    select
        *
        , global_alias.alias_search_text is not null as is_reviewed_global_location
        , ifNull(raw_country_eligibility_scope, 'UNKNOWN') not in ('GLOBAL', 'GLOBAL_EXCEPT')
            and (
                (
                    country_field_role = 'JOB_LOCATION'
                    and classification_remote_scope = 'ONSITE'
                )
                or (
                    ifNull(can_restrict, 1)
                    and (
                        is_reviewed_platform_location_role
                        or (
                            country_field_role = 'JOB_LOCATION'
                            and classification_remote_scope in ('REMOTE', 'HYBRID')
                            and lowerUTF8(ifNull(source_platform_guess, '')) in ('lever', 'workday')
                        )
                    )
                )
            ) as is_restricting_location_evidence
    from prepared_roles
    left join global_location_aliases as global_alias
        on global_alias.source_platform_guess = lowerUTF8(ifNull(source_platform_guess, ''))
        and global_alias.alias_search_text = normalized_raw_value
),

final as (
    select
        concat(
            candidate_id
            , '|', stage_run_id
            , '|', classification_run_id
            , '|', source_artifact_sha256
            , '|', toString(source_record_index)
        ) as evidence_id
        , landing_run_id as source_landing_run_id
        , candidate_id
        , stage_run_id
        , classification_run_id
        , source_record_index
        , source_artifact_sha256
        , country_field_role
        , country_field_source
        , country_field_source_system
        , source_platform_guess
        , raw_country_eligibility_scope
        , classification_country_scope
        , classification_remote_scope
        , raw_value
        , json_path
        , quote
        , rule
        , can_restrict
        , normalized_raw_value
        , case_sensitive_search_text
        , location_object_path
        , is_location_evidence_role
        , is_reviewed_platform_location_role
        , is_reviewed_global_location
        , is_restricting_location_evidence
        , case
            when is_reviewed_global_location then 'GLOBAL'
            when is_restricting_location_evidence then 'INCLUDED'
            when ifNull(raw_country_eligibility_scope, 'UNKNOWN') in ('GLOBAL', 'GLOBAL_EXCEPT')
                and country_field_role in (
                    'APPLICANT_LOCATION_REQUIREMENTS'
                    , 'LLM_GLOBAL_SCOPE'
                    , 'NORMALIZED_TEXT'
                    , 'SOURCE_DEFAULT_COUNTRY_ELIGIBILITY'
                ) then 'GLOBAL'
            when country_field_role in ('LLM_EXCLUDED_COUNTRY', 'LLM_EXCLUDED_GROUP')
                then 'EXCLUDED'
            when country_field_role in ('LLM_INCLUDED_COUNTRY', 'LLM_INCLUDED_GROUP')
                then 'INCLUDED'
            when country_field_role in ('LLM_UNKNOWN', 'LLM_INVALID_OUTPUT', 'NO_COUNTRY_EVIDENCE')
                then 'UNKNOWN'
            when country_field_role in (
                    'APPLICANT_LOCATION_REQUIREMENTS'
                    , 'NORMALIZED_TEXT'
                    , 'SOURCE_DEFAULT_COUNTRY_ELIGIBILITY'
                )
                and ifNull(can_restrict, 1) then 'INCLUDED'
            else 'UNKNOWN'
        end as evidence_direction
        , case
            when is_restricting_location_evidence
                and country_field_role = 'JOB_LOCATION'
                and endsWith(lowerUTF8(ifNull(json_path, '')), '.addresscountry')
                then 'ATOMIC'
            when is_restricting_location_evidence then 'TEXT'
            when country_field_role in (
                    'APPLICANT_LOCATION_REQUIREMENTS'
                    , 'LLM_EXCLUDED_COUNTRY'
                    , 'LLM_INCLUDED_COUNTRY'
                    , 'SOURCE_DEFAULT_COUNTRY_ELIGIBILITY'
                ) then 'ATOMIC'
            when country_field_role = 'NORMALIZED_TEXT' then 'TEXT'
            else 'NONE'
        end as country_match_mode
    from prepared_inputs
)

select *
from final

select
    inputs.evidence_id
    , uniqExactIf(
        matches.matched_country_code
        , notEmpty(ifNull(matches.matched_country_code, ''))
    ) as matched_country_count
    , uniqExactIf(
        matches.matched_country_group_code
        , notEmpty(ifNull(matches.matched_country_group_code, ''))
    ) as matched_country_group_count
from {{ ref('int_wremotely__country_eligibility_inputs') }} as inputs
left join {{ ref('int_wremotely__country_eligibility_exact_matches') }} as matches
    on inputs.evidence_id = matches.evidence_id
where inputs.country_field_role in (
    'APPLICANT_LOCATION_REQUIREMENTS'
    , 'LLM_EXCLUDED_COUNTRY'
    , 'LLM_EXCLUDED_GROUP'
    , 'LLM_INCLUDED_COUNTRY'
    , 'LLM_INCLUDED_GROUP'
    , 'SOURCE_DEFAULT_COUNTRY_ELIGIBILITY'
)
group by inputs.evidence_id
having matched_country_count > 1
    or matched_country_group_count > 1

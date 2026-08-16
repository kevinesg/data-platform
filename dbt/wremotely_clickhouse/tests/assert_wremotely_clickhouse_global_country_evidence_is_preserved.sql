select
    evidence.candidate_id
    , evidence.evidence_id
    , evidence.evidence_direction
from {{ ref('int_wremotely__country_eligibility_inputs') }} as evidence
where evidence.raw_country_eligibility_scope in ('GLOBAL', 'GLOBAL_EXCEPT')
    and evidence.country_field_role in (
        'APPLICANT_LOCATION_REQUIREMENTS'
        , 'LLM_GLOBAL_SCOPE'
        , 'NORMALIZED_TEXT'
        , 'SOURCE_DEFAULT_COUNTRY_ELIGIBILITY'
    )
    and evidence.evidence_direction != 'GLOBAL'

select
    candidate_id
    , country_field_role
    , raw_value
    , evidence_direction
from {{ ref('int_wremotely__country_eligibility_inputs') }}
where country_field_role = 'JOB_LOCATION'
    and ifNull(classification_remote_scope, '') != 'ONSITE'
    and evidence_direction not in ('UNKNOWN', 'GLOBAL')
    and not (
        classification_remote_scope in ('REMOTE', 'HYBRID')
        and lowerUTF8(ifNull(source_platform_guess, '')) in ('lever', 'workday')
        and ifNull(can_restrict, false)
    )

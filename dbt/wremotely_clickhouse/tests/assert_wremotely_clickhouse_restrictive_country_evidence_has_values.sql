select
    candidate_id
    , country_field_role
    , evidence_direction
    , raw_value
from {{ ref('int_wremotely__country_eligibility_inputs') }}
where evidence_direction in ('INCLUDED', 'EXCLUDED')
    and empty(trim(ifNull(raw_value, '')))

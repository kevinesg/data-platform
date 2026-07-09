SELECT
    candidate_id
    , country_field_role
    , raw_value
    , evidence_direction
    , match_source
FROM {{ ref('int_wremotely__country_eligibility_evidence') }}
WHERE country_field_role = 'JOB_LOCATION'
    AND evidence_direction != 'UNKNOWN'

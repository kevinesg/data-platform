SELECT
    candidate_id
    , country_field_role
    , evidence_direction
    , raw_value
    , match_source
FROM {{ ref('int_wremotely__country_eligibility_evidence') }}
WHERE evidence_direction IN ('INCLUDED', 'EXCLUDED')
    AND NULLIF(TRIM(raw_value), '') IS NULL

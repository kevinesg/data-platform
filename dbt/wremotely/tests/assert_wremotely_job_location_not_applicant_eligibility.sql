SELECT
    candidate_id
    , country_field_role
    , raw_value
    , evidence_direction
    , match_source
FROM {{ ref('int_wremotely__country_eligibility_evidence') }}
WHERE country_field_role = 'JOB_LOCATION'
    AND COALESCE(classification_remote_scope, '') != 'ONSITE'
    AND evidence_direction != 'UNKNOWN'
    AND NOT (
        (
            (
                classification_remote_scope IN ('REMOTE', 'HYBRID')
                AND LOWER(COALESCE(source_platform_guess, '')) = 'lever'
            )
            OR (
                classification_remote_scope IN ('REMOTE', 'HYBRID')
                AND LOWER(COALESCE(source_platform_guess, '')) = 'workday'
            )
        )
        AND COALESCE(can_restrict, FALSE)
    )

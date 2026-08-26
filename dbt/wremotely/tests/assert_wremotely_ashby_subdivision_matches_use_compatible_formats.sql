SELECT
    candidate_id
    , country_field_role
    , json_path
    , source_platform_guess
    , raw_value
FROM {{ ref('int_wremotely__country_eligibility_evidence') }}
WHERE LOWER(COALESCE(source_platform_guess, '')) = 'ashby'
    AND match_source = 'COUNTRY_SUBDIVISION_TEXT_ALIAS'
    AND NOT (
        country_field_role = 'JOB_LOCATION'
        AND ENDS_WITH(LOWER(COALESCE(json_path, '')), '.addressregion')
    )

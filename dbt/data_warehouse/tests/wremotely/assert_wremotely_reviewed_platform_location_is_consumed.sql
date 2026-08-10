SELECT
    candidate_id
    , country_field_role
    , country_field_source_system
    , source_platform_guess
    , raw_value
    , evidence_direction
FROM {{ ref('int_wremotely__country_eligibility_evidence') }}
WHERE COALESCE(can_restrict, FALSE)
    AND raw_country_eligibility_scope NOT IN ('GLOBAL', 'GLOBAL_EXCEPT')
    AND evidence_direction = 'UNKNOWN'
    AND (
        (
            country_field_role = 'PLATFORM_JOB_LOCATION'
            AND LOWER(COALESCE(source_platform_guess, '')) IN (
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
        )
        OR (
            country_field_role = country_field_source_system
            AND country_field_role IN (
                'BAMBOOHR_CAREERS_LIST'
                , 'BREEZY_META'
                , 'GREENHOUSE_REMIX'
                , 'JAZZHR_VISIBLE_HTML'
                , 'JOBVITE_PRELOADED_DATA'
                , 'NEXTJS'
                , 'PERSONIO_VISIBLE_HTML'
                , 'SMARTRECRUITERS_MICRODATA'
                , 'WORKABLE_WIDGET'
            )
        )
    )

select
    candidate_id
    , country_field_role
    , country_field_source_system
    , source_platform_guess
    , raw_value
    , evidence_direction
from {{ ref('int_wremotely__country_eligibility_inputs') }}
where ifNull(can_restrict, false)
    and raw_country_eligibility_scope not in ('GLOBAL', 'GLOBAL_EXCEPT')
    and evidence_direction = 'UNKNOWN'
    and (
        (
            country_field_role = 'PLATFORM_JOB_LOCATION'
            and lowerUTF8(ifNull(source_platform_guess, '')) in (
                'ashby', 'bamboohr', 'breezy', 'greenhouse', 'jazzhr', 'jobvite'
                , 'lever', 'personio', 'rippling', 'smartrecruiters', 'workable'
            )
        )
        or (
            country_field_role = country_field_source_system
            and country_field_role in (
                'BAMBOOHR_CAREERS_LIST', 'BREEZY_META', 'GREENHOUSE_REMIX'
                , 'JAZZHR_VISIBLE_HTML', 'JOBVITE_PRELOADED_DATA', 'NEXTJS'
                , 'PERSONIO_VISIBLE_HTML', 'SMARTRECRUITERS_MICRODATA'
                , 'WORKABLE_WIDGET'
            )
        )
    )

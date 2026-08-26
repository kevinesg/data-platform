select
    m.candidate_id
    , i.country_field_role
    , i.json_path
    , i.source_platform_guess
    , i.raw_value
from {{ ref('int_wremotely__country_eligibility_subdivision_text_matches') }} as m
inner join {{ ref('int_wremotely__country_eligibility_inputs') }} as i
    on m.candidate_id = i.candidate_id
    and m.evidence_id = i.evidence_id
where lowerUTF8(ifNull(i.source_platform_guess, '')) = 'ashby'
    and not (
        i.country_field_role = 'JOB_LOCATION'
        and endsWith(lowerUTF8(ifNull(i.json_path, '')), '.addressregion')
    )

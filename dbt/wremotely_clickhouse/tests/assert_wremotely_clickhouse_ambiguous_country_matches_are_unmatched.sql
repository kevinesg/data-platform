select
    evidence_id
    , matched_country_code
    , matched_country_group_code
from {{ ref('int_wremotely__country_eligibility_exact_matches') }}
where match_status = 'AMBIGUOUS_COUNTRY_ALIAS'
    and (
        notEmpty(ifNull(matched_country_code, ''))
        or notEmpty(ifNull(matched_country_group_code, ''))
    )

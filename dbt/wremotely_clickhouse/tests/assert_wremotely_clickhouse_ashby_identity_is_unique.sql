select
    lowerUTF8(selected_normalized_url) as canonical_url
    , count() as candidate_count
from {{ ref('int_wremotely__current_candidate_facts') }}
where lowerUTF8(ifNull(selected_source_platform_guess, '')) = 'ashby'
  and selected_normalized_url is not null
group by canonical_url
having candidate_count > 1

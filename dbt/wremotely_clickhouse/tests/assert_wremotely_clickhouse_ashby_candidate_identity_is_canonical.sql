select
    candidate_id
    , selected_normalized_url
from {{ ref('int_wremotely__current_candidate_facts') }}
where lowerUTF8(ifNull(selected_source_platform_guess, '')) = 'ashby'
  and selected_normalized_url is not null
  and candidate_id != lower(hex(SHA256(lowerUTF8(selected_normalized_url))))

select
    selected.candidate_id
    , selected.url
from {{ ref('int_wremotely__latest_selected_job_urls') }} as selected
left join {{ ref('int_wremotely__current_candidate_facts') }} as current_facts
    on selected.candidate_id = current_facts.candidate_id
where current_facts.candidate_id is null
    or nullIf(trim(selected.url), '') != nullIf(trim(current_facts.url), '')

select
    evidence.candidate_id
    , evidence.stage_run_id
    , evidence.classification_run_id
    , latest.latest_classification_stage_run_id
    , latest.latest_classification_run_id
from {{ ref('int_wremotely__country_eligibility_inputs') }} as evidence
inner join {{ ref('int_wremotely__latest_classifications') }} as latest
    on evidence.candidate_id = latest.candidate_id
where evidence.stage_run_id != latest.latest_classification_stage_run_id
    or evidence.classification_run_id != latest.latest_classification_run_id

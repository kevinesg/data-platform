SELECT
    evidence.candidate_id
    , evidence.stage_run_id
    , evidence.classification_run_id
    , latest.latest_classification_stage_run_id
    , latest.latest_classification_run_id
FROM {{ ref('int_wremotely__country_eligibility_evidence') }} AS evidence
INNER JOIN {{ ref('int_wremotely__latest_classifications') }} AS latest
    ON evidence.candidate_id = latest.candidate_id
WHERE evidence.stage_run_id != latest.latest_classification_stage_run_id
    OR evidence.classification_run_id != latest.latest_classification_run_id

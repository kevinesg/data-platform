SELECT
    candidate_id
    , stage_run_id
    , classification_run_id
    , source_record_index
    , source_evidence_index
    , COUNT(DISTINCT matched_country_code) AS matched_country_count
    , COUNT(DISTINCT matched_country_group_code) AS matched_country_group_count
FROM {{ ref('int_wremotely__country_eligibility_evidence') }}
WHERE country_field_role IN (
    'APPLICANT_LOCATION_REQUIREMENTS'
    , 'LLM_EXCLUDED_COUNTRY'
    , 'LLM_EXCLUDED_GROUP'
    , 'LLM_INCLUDED_COUNTRY'
    , 'LLM_INCLUDED_GROUP'
    , 'SOURCE_DEFAULT_COUNTRY_ELIGIBILITY'
)
GROUP BY
    candidate_id
    , stage_run_id
    , classification_run_id
    , source_record_index
    , source_evidence_index
HAVING matched_country_count > 1
    OR matched_country_group_count > 1

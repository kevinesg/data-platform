WITH latest_raw_global_evidence AS (
    SELECT raw.*
    FROM {{ ref('stg_wremotely__country_eligibility_extractions') }} AS raw
    INNER JOIN {{ ref('int_wremotely__latest_classifications') }} AS latest
        ON raw.candidate_id = latest.candidate_id
        AND raw.stage_run_id = latest.latest_classification_stage_run_id
        AND raw.classification_run_id = latest.latest_classification_run_id
    WHERE raw.raw_country_eligibility_scope IN ('GLOBAL', 'GLOBAL_EXCEPT')
        AND raw.country_field_role IN (
            'APPLICANT_LOCATION_REQUIREMENTS'
            , 'LLM_GLOBAL_SCOPE'
            , 'NORMALIZED_TEXT'
            , 'SOURCE_DEFAULT_COUNTRY_ELIGIBILITY'
        )
)

SELECT
    raw.candidate_id
    , raw.stage_run_id
    , raw.classification_run_id
    , raw.source_record_index
    , raw.source_evidence_index
FROM latest_raw_global_evidence AS raw
LEFT JOIN {{ ref('int_wremotely__country_eligibility_evidence') }} AS evidence
    ON raw.candidate_id = evidence.candidate_id
    AND raw.stage_run_id = evidence.stage_run_id
    AND raw.classification_run_id = evidence.classification_run_id
    AND raw.source_record_index = evidence.source_record_index
    AND COALESCE(raw.source_evidence_index, 0) = COALESCE(evidence.source_evidence_index, 0)
    AND evidence.evidence_direction = 'GLOBAL'
WHERE evidence.candidate_id IS NULL

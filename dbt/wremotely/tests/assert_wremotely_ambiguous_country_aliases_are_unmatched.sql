SELECT
    candidate_id
    , stage_run_id
    , classification_run_id
    , source_record_index
    , source_evidence_index
    , matched_country_code
    , matched_country_group_code
FROM {{ ref('int_wremotely__country_eligibility_evidence') }}
WHERE match_source = 'AMBIGUOUS_COUNTRY_ALIAS'
    AND (
        matched_country_code IS NOT NULL
        OR matched_country_group_code IS NOT NULL
    )

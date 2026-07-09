WITH raw_publication_holds AS (
    SELECT *
    FROM {{ source('wremotely', 'publication_holds') }}
),

renamed AS (
    SELECT
        stage_run_id
        , source_run_id AS publication_hold_run_id
        , source_record_index
        , source_artifact_sha256
        , contract_version AS raw_contract_version
        , SAFE_CAST(JSON_VALUE(payload, '$.contract_version') AS INT64)
            AS payload_contract_version
        , JSON_VALUE(payload, '$.candidate_id') AS candidate_id
        , JSON_VALUE(payload, '$.url') AS url
        , SAFE_CAST(JSON_VALUE(payload, '$.evaluated_at') AS TIMESTAMP) AS evaluated_at
        , TRIM(UPPER(JSON_VALUE(payload, '$.hold_status'))) AS hold_status
        , TRIM(UPPER(JSON_VALUE(payload, '$.hold_reason_code'))) AS hold_reason_code
        , TRIM(UPPER(JSON_VALUE(payload, '$.hold_guardrail_reason'))) AS hold_guardrail_reason
        , JSON_VALUE(payload, '$.hold_evidence_quote') AS hold_evidence_quote
        , TRIM(UPPER(JSON_VALUE(payload, '$.classification_remote_scope')))
            AS classification_remote_scope
        , TRIM(UPPER(JSON_VALUE(payload, '$.classification_country_eligibility_scope')))
            AS classification_country_eligibility_scope
        , TRIM(UPPER(JSON_VALUE(payload, '$.classification_serving_decision')))
            AS classification_serving_decision
        , JSON_VALUE(payload, '$.declared_language_raw') AS declared_language_raw
        , LOWER(NULLIF(TRIM(JSON_VALUE(payload, '$.declared_language_tag')), ''))
            AS declared_language_tag
        , JSON_VALUE(payload, '$.declared_language_source') AS declared_language_source
        , JSON_VALUE(payload, '$.source_candidate_id') AS source_candidate_id
        , JSON_VALUE(payload, '$.source_url') AS source_url
        , JSON_VALUE(payload, '$.source_url_identity') AS source_url_identity
        , TRIM(UPPER(JSON_VALUE(payload, '$.source_type_guess'))) AS source_type_guess
        , JSON_VALUE(payload, '$.source_platform_guess') AS source_platform_guess
        , TRIM(UPPER(JSON_VALUE(payload, '$.source_review_status'))) AS source_review_status
        , TRIM(UPPER(JSON_VALUE(payload, '$.source_default_work_arrangement')))
            AS source_default_work_arrangement
        , JSON_VALUE(payload, '$.source_content_sha256') AS source_content_sha256
        , JSON_VALUE(payload, '$.normalized_text_sha256') AS normalized_text_sha256
        , JSON_VALUE(payload, '$.jsonld_sha256') AS jsonld_sha256
        , JSON_VALUE(payload, '$.policy_sha256') AS policy_sha256
        , JSON_VALUE(payload, '$.publication_hold_evaluator_version')
            AS publication_hold_evaluator_version
        , JSON_VALUE(payload, '$.llm_response_sha256') AS llm_response_sha256
        , JSON_VALUE(payload, '$.llm_skipped_reason') AS llm_skipped_reason
        , payload AS raw_payload
    FROM raw_publication_holds
)

SELECT *
FROM renamed

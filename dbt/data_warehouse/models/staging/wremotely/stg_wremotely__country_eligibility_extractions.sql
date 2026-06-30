WITH raw_country_eligibility_extractions AS (
    SELECT *
    FROM {{ source('wremotely', 'country_eligibility_extractions') }}
),

renamed AS (
    SELECT
        stage_run_id
        , source_run_id AS classification_run_id
        , source_record_index
        , source_artifact_sha256
        , contract_version AS raw_contract_version
        , SAFE_CAST(JSON_VALUE(payload, '$.contract_version') AS INT64) AS payload_contract_version
        , JSON_VALUE(payload, '$.candidate_id') AS candidate_id
        , JSON_VALUE(payload, '$.url') AS url
        , SAFE_CAST(JSON_VALUE(payload, '$.extracted_at') AS TIMESTAMP) AS extracted_at
        , JSON_VALUE(payload, '$.country_eligibility_extractor_version') AS country_eligibility_extractor_version
        , JSON_VALUE(payload, '$.classifier_version') AS classifier_version
        , JSON_VALUE(payload, '$.target_country') AS target_country
        , TRIM(UPPER(JSON_VALUE(payload, '$.target_country_code'))) AS target_country_code
        , TRIM(UPPER(JSON_VALUE(payload, '$.target_country_eligibility'))) AS target_country_eligibility
        , TRIM(UPPER(JSON_VALUE(payload, '$.country_eligibility_scope'))) AS raw_country_eligibility_scope
        , JSON_VALUE(payload, '$.source_content_sha256') AS source_content_sha256
        , JSON_VALUE(payload, '$.normalized_text_sha256') AS normalized_text_sha256
        , JSON_VALUE(payload, '$.jsonld_sha256') AS jsonld_sha256
        , JSON_VALUE(payload, '$.page_extractor') AS page_extractor
        , SAFE_CAST(JSON_VALUE(payload, '$.source_evidence_index') AS INT64) AS source_evidence_index
        , TRIM(UPPER(JSON_VALUE(payload, '$.country_field_source'))) AS country_field_source
        , TRIM(UPPER(JSON_VALUE(payload, '$.country_field_role'))) AS country_field_role
        , TRIM(UPPER(JSON_VALUE(payload, '$.country_field_source_system'))) AS country_field_source_system
        , JSON_VALUE(payload, '$.country_field_source_identifier') AS country_field_source_identifier
        , TRIM(UPPER(JSON_VALUE(payload, '$.extraction_method'))) AS extraction_method
        , TRIM(UPPER(JSON_VALUE(payload, '$.raw_value_field'))) AS raw_value_field
        , JSON_VALUE(payload, '$.raw_value') AS raw_value
        , JSON_VALUE(payload, '$.json_path') AS json_path
        , JSON_VALUE(payload, '$.quote') AS quote
        , TRIM(UPPER(JSON_VALUE(payload, '$.rule'))) AS rule
        , SAFE_CAST(JSON_VALUE(payload, '$.can_restrict') AS BOOL) AS can_restrict
        , TRIM(UPPER(JSON_VALUE(payload, '$.llm_runtime'))) AS llm_runtime
        , JSON_VALUE(payload, '$.llm_model') AS llm_model
        , JSON_VALUE(payload, '$.llm_prompt_version') AS llm_prompt_version
        , JSON_VALUE(payload, '$.llm_response') AS llm_response
        , payload AS raw_payload
    FROM raw_country_eligibility_extractions
)

SELECT *
FROM renamed

WITH raw_classifications AS (
    SELECT *
    FROM {{ source('wremotely', 'classification_classifications') }}
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
        , SAFE_CAST(JSON_VALUE(payload, '$.classified_at') AS TIMESTAMP) AS classified_at
        , JSON_VALUE(payload, '$.classifier_version') AS classifier_version
        , JSON_VALUE(payload, '$.model') AS model
        , TRIM(UPPER(JSON_VALUE(payload, '$.classification_status'))) AS classification_status
        , TRIM(UPPER(JSON_VALUE(payload, '$.job_posting_type'))) AS job_posting_type
        , TRIM(UPPER(JSON_VALUE(payload, '$.job_status'))) AS job_status
        , TRIM(UPPER(JSON_VALUE(payload, '$.remote_scope'))) AS remote_scope
        , CASE TRIM(UPPER(JSON_VALUE(payload, '$.country_eligibility_scope')))
            WHEN 'TARGET_COUNTRY' THEN 'SPECIFIC'
            WHEN 'RESTRICTED' THEN 'SPECIFIC'
            ELSE COALESCE(
                TRIM(UPPER(JSON_VALUE(payload, '$.country_eligibility_scope')))
                , 'UNKNOWN'
            )
        END AS country_eligibility_scope
        , JSON_VALUE(payload, '$.target_country') AS target_country
        , TRIM(UPPER(JSON_VALUE(payload, '$.target_country_code'))) AS target_country_code
        , TRIM(UPPER(JSON_VALUE(payload, '$.target_country_eligibility'))) AS target_country_eligibility
        , TRIM(UPPER(JSON_VALUE(payload, '$.serving_decision'))) AS serving_decision
        , JSON_VALUE(payload, '$.source_candidate_id') AS source_candidate_id
        , JSON_VALUE(payload, '$.source_url') AS source_url
        , JSON_VALUE(payload, '$.source_url_identity') AS source_url_identity
        , TRIM(UPPER(JSON_VALUE(payload, '$.source_type_guess'))) AS source_type_guess
        , JSON_VALUE(payload, '$.source_platform_guess') AS source_platform_guess
        , TRIM(UPPER(JSON_VALUE(payload, '$.source_review_status'))) AS source_review_status
        , TRIM(UPPER(JSON_VALUE(payload, '$.source_default_work_arrangement'))) AS source_default_work_arrangement
        , JSON_VALUE(payload, '$.source_content_sha256') AS source_content_sha256
        , JSON_VALUE(payload, '$.normalized_text_sha256') AS normalized_text_sha256
        , JSON_VALUE(payload, '$.jsonld_sha256') AS jsonld_sha256
        , JSON_VALUE(payload, '$.declared_language_raw') AS declared_language_raw
        , JSON_VALUE(payload, '$.declared_language_tag') AS declared_language_tag
        , JSON_VALUE(payload, '$.declared_language_source') AS declared_language_source
        , JSON_QUERY_ARRAY(payload, '$.evidence') AS evidence_json
    FROM raw_classifications
)

SELECT *
FROM renamed

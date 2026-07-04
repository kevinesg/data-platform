WITH raw_job_url_selection_results AS (
    SELECT *
    FROM {{ source('wremotely', 'job_url_selection_results') }}
),

renamed AS (
    SELECT
        stage_run_id
        , source_run_id AS selection_run_id
        , source_record_index
        , source_artifact_sha256
        , contract_version AS raw_contract_version
        , SAFE_CAST(JSON_VALUE(payload, '$.contract_version') AS INT64) AS payload_contract_version
        , JSON_VALUE(payload, '$.job_url_id') AS job_url_id
        , JSON_VALUE(payload, '$.candidate_id') AS candidate_id
        , JSON_VALUE(payload, '$.url') AS url
        , JSON_VALUE(payload, '$.raw_url') AS raw_url
        , JSON_VALUE(payload, '$.normalized_url') AS normalized_url
        , JSON_VALUE(payload, '$.source_candidate_id') AS source_candidate_id
        , JSON_VALUE(payload, '$.source_url') AS source_url
        , JSON_VALUE(payload, '$.source_domain') AS source_domain
        , TRIM(UPPER(JSON_VALUE(payload, '$.source_candidate_kind'))) AS source_candidate_kind
        , JSON_VALUE(payload, '$.source_url_identity') AS source_url_identity
        , TRIM(UPPER(JSON_VALUE(payload, '$.source_type_guess'))) AS source_type_guess
        , JSON_VALUE(payload, '$.source_platform_guess') AS source_platform_guess
        , TRIM(UPPER(JSON_VALUE(payload, '$.source_review_status'))) AS source_review_status
        , TRIM(UPPER(JSON_VALUE(payload, '$.source_default_work_arrangement'))) AS source_default_work_arrangement
        , JSON_VALUE(payload, '$.source_page_final_url') AS source_page_final_url
        , SAFE_CAST(JSON_VALUE(payload, '$.link_index') AS INT64) AS link_index
        , JSON_VALUE(payload, '$.link_text') AS link_text
        , JSON_VALUE(payload, '$.link_rel') AS link_rel
        , TRIM(UPPER(JSON_VALUE(payload, '$.discovery_reason'))) AS discovery_reason
        , JSON_VALUE(payload, '$.crawler_version') AS crawler_version
        , JSON_VALUE(payload, '$.source_crawl_run_id') AS source_crawl_run_id
        , SAFE_CAST(JSON_VALUE(payload, '$.selected_at') AS TIMESTAMP) AS selected_at
        , JSON_VALUE(payload, '$.selector_version') AS selector_version
        , TRIM(UPPER(JSON_VALUE(payload, '$.selection_status'))) AS selection_status
        , COALESCE(
            TRIM(UPPER(JSON_VALUE(payload, '$.selection_reason')))
            , CASE TRIM(UPPER(JSON_VALUE(payload, '$.selection_status')))
                WHEN 'SELECTED' THEN 'NEW_URL_SELECTED'
                WHEN 'SKIPPED_KNOWN_URL' THEN 'KNOWN_URL_IDENTITY'
                WHEN 'SKIPPED_DUPLICATE_URL_IDENTITY' THEN 'DUPLICATE_WITHIN_SOURCE_CRAWL'
                WHEN 'SKIPPED_SELECTION_LIMIT' THEN 'SELECTION_LIMIT_REACHED'
                ELSE 'UNKNOWN_SELECTION_STATUS'
            END
        ) AS selection_reason
        , COALESCE(
            SAFE_CAST(JSON_VALUE(payload, '$.known_url_match') AS BOOL)
            , false
        ) AS known_url_match
        , SAFE_CAST(
            JSON_VALUE(payload, '$.duplicate_url_identity') AS BOOL
        ) IS TRUE AS duplicate_url_identity
        , JSON_VALUE(payload, '$.duplicate_of_job_url_id') AS duplicate_of_job_url_id
    FROM raw_job_url_selection_results
)

SELECT *
FROM renamed

WITH raw_source_crawl_job_urls AS (
    SELECT *
    FROM {{ source('wremotely', 'source_crawl_job_urls') }}
),

renamed AS (
    SELECT
        stage_run_id
        , source_run_id AS source_crawl_run_id
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
    FROM raw_source_crawl_job_urls
)

SELECT *
FROM renamed

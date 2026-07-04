WITH raw_source_crawl_pages AS (
    SELECT *
    FROM {{ source('wremotely', 'source_crawl_pages') }}
),

renamed AS (
    SELECT
        stage_run_id
        , source_run_id AS source_crawl_run_id
        , source_record_index
        , source_artifact_sha256
        , contract_version AS raw_contract_version
        , SAFE_CAST(JSON_VALUE(payload, '$.contract_version') AS INT64) AS payload_contract_version
        , JSON_VALUE(payload, '$.source_candidate_id') AS source_candidate_id
        , JSON_VALUE(payload, '$.source_url') AS source_url
        , JSON_VALUE(payload, '$.normalized_source_url') AS normalized_source_url
        , JSON_VALUE(payload, '$.source_domain') AS source_domain
        , TRIM(UPPER(JSON_VALUE(payload, '$.source_candidate_kind'))) AS source_candidate_kind
        , JSON_VALUE(payload, '$.source_url_identity') AS source_url_identity
        , TRIM(UPPER(JSON_VALUE(payload, '$.source_type_guess'))) AS source_type_guess
        , JSON_VALUE(payload, '$.source_platform_guess') AS source_platform_guess
        , TRIM(UPPER(JSON_VALUE(payload, '$.source_review_status'))) AS source_review_status
        , TRIM(UPPER(JSON_VALUE(payload, '$.source_default_work_arrangement'))) AS source_default_work_arrangement
        , TRIM(UPPER(JSON_VALUE(payload, '$.status'))) AS page_status
        , SAFE_CAST(JSON_VALUE(payload, '$.crawled_at') AS TIMESTAMP) AS crawled_at
        , JSON_VALUE(payload, '$.crawler_version') AS crawler_version
        , SAFE_CAST(JSON_VALUE(payload, '$.http_status') AS INT64) AS http_status
        , JSON_VALUE(payload, '$.final_url') AS final_url
        , JSON_VALUE(payload, '$.normalized_final_url') AS normalized_final_url
        , JSON_QUERY_ARRAY(payload, '$.redirect_chain') AS redirect_chain_json
        , JSON_VALUE(payload, '$.content_type') AS content_type
        , SAFE_CAST(JSON_VALUE(payload, '$.attempt_count') AS INT64) AS attempt_count
        , SAFE_CAST(JSON_VALUE(payload, '$.robots_txt_allowed') AS BOOL) AS robots_txt_allowed
        , TRIM(UPPER(JSON_VALUE(payload, '$.robots_txt_status'))) AS robots_txt_status
        , SAFE_CAST(
            JSON_VALUE(payload, '$.robots_txt_http_status') AS INT64
        ) AS robots_txt_http_status
        , JSON_VALUE(payload, '$.robots_txt_url') AS robots_txt_url
        , JSON_VALUE(payload, '$.robots_txt_error') AS robots_txt_error
        , JSON_QUERY(payload, '$.robots_txt') AS robots_txt_json
        , TRIM(UPPER(JSON_VALUE(payload, '$.error_type'))) AS error_type
        , JSON_VALUE(payload, '$.error') AS error
        , JSON_VALUE(payload, '$.content_sha256') AS content_sha256
        , SAFE_CAST(JSON_VALUE(payload, '$.crawl_depth') AS INT64) AS crawl_depth
        , JSON_VALUE(payload, '$.parent_source_url') AS parent_source_url
        , SAFE_CAST(JSON_VALUE(payload, '$.extracted_link_count') AS INT64) AS extracted_link_count
        , SAFE_CAST(
            JSON_VALUE(payload, '$.discovered_job_url_count') AS INT64
        ) AS discovered_job_url_count
        , SAFE_CAST(
            JSON_VALUE(payload, '$.discovered_pagination_url_count') AS INT64
        ) AS discovered_pagination_url_count
        , TRIM(UPPER(JSON_VALUE(payload, '$.pagination_reason'))) AS pagination_reason
        , TRIM(
            UPPER(JSON_VALUE(payload, '$.platform_adapter_error_type'))
        ) AS platform_adapter_error_type
        , JSON_VALUE(payload, '$.platform_adapter_error') AS platform_adapter_error
        , TRIM(
            UPPER(JSON_VALUE(payload, '$.source_crawl_page_stop_reason'))
        ) AS source_crawl_page_stop_reason
        , TRIM(
            UPPER(JSON_VALUE(payload, '$.source_crawl_page_next_action'))
        ) AS source_crawl_page_next_action
    FROM raw_source_crawl_pages
)

SELECT *
FROM renamed

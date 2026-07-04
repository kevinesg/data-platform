WITH raw_page_results AS (
    SELECT *
    FROM {{ source('wremotely', 'extraction_page_results') }}
),

renamed AS (
    SELECT
        stage_run_id
        , source_run_id AS extraction_run_id
        , source_record_index
        , source_artifact_sha256
        , contract_version AS raw_contract_version
        , SAFE_CAST(JSON_VALUE(payload, '$.contract_version') AS INT64) AS payload_contract_version
        , JSON_VALUE(payload, '$.candidate_id') AS candidate_id
        , JSON_VALUE(payload, '$.url') AS url
        , JSON_VALUE(payload, '$.source_domain') AS source_domain
        , TRIM(UPPER(JSON_VALUE(payload, '$.status'))) AS page_status
        , SAFE_CAST(JSON_VALUE(payload, '$.retrieved_at') AS TIMESTAMP) AS retrieved_at
        , SAFE_CAST(JSON_VALUE(payload, '$.http_status') AS INT64) AS http_status
        , JSON_VALUE(payload, '$.final_url') AS final_url
        , JSON_QUERY_ARRAY(payload, '$.redirect_chain') AS redirect_chain_json
        , JSON_VALUE(payload, '$.content_type') AS content_type
        , SAFE_CAST(JSON_VALUE(payload, '$.attempt_count') AS INT64) AS attempt_count
        , TRIM(UPPER(JSON_VALUE(payload, '$.extractor'))) AS extractor
        , TRIM(UPPER(JSON_VALUE(payload, '$.primary_extractor'))) AS primary_extractor
        , JSON_QUERY(payload, '$.primary_result') AS primary_result_json
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
        , JSON_VALUE(payload, '$.raw_html_path') AS raw_html_path
        , JSON_VALUE(payload, '$.normalized_text_path') AS normalized_text_path
        , JSON_VALUE(payload, '$.normalized_text_sha256') AS normalized_text_sha256
        , SAFE_CAST(
            JSON_VALUE(payload, '$.normalized_text_char_count') AS INT64
        ) AS normalized_text_char_count
        , JSON_VALUE(payload, '$.jsonld_path') AS jsonld_path
        , JSON_VALUE(payload, '$.jsonld_sha256') AS jsonld_sha256
        , SAFE_CAST(
            JSON_VALUE(payload, '$.jsonld_document_count') AS INT64
        ) AS jsonld_document_count
        , SAFE_CAST(
            JSON_VALUE(payload, '$.jsonld_parse_error_count') AS INT64
        ) AS jsonld_parse_error_count
    FROM raw_page_results
)

SELECT *
FROM renamed

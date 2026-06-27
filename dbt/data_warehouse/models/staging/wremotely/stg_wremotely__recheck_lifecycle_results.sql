WITH raw_lifecycle_results AS (
    SELECT *
    FROM {{ source('wremotely', 'recheck_lifecycle_results') }}
),

renamed AS (
    SELECT
        stage_run_id
        , source_run_id AS recheck_run_id
        , source_record_index
        , source_artifact_sha256
        , contract_version AS raw_contract_version
        , SAFE_CAST(JSON_VALUE(payload, '$.contract_version') AS INT64) AS payload_contract_version
        , JSON_VALUE(payload, '$.candidate_id') AS candidate_id
        , JSON_VALUE(payload, '$.url') AS url
        , JSON_VALUE(payload, '$.source_domain') AS source_domain
        , SAFE_CAST(JSON_VALUE(payload, '$.checked_at') AS TIMESTAMP) AS checked_at
        , JSON_VALUE(payload, '$.checker_version') AS checker_version
        , JSON_VALUE(payload, '$.page_status') AS page_status
        , JSON_VALUE(payload, '$.lifecycle_status') AS lifecycle_status
        , JSON_VALUE(payload, '$.lifecycle_signal') AS lifecycle_signal
        , SAFE_CAST(JSON_VALUE(payload, '$.http_status') AS INT64) AS http_status
        , JSON_VALUE(payload, '$.final_url') AS final_url
        , JSON_QUERY_ARRAY(payload, '$.redirect_chain') AS redirect_chain_json
        , JSON_VALUE(payload, '$.content_type') AS content_type
        , SAFE_CAST(JSON_VALUE(payload, '$.attempt_count') AS INT64) AS attempt_count
        , JSON_VALUE(payload, '$.extractor') AS extractor
        , SAFE_CAST(JSON_VALUE(payload, '$.robots_txt_allowed') AS BOOL) AS robots_txt_allowed
        , JSON_VALUE(payload, '$.robots_txt_status') AS robots_txt_status
        , SAFE_CAST(
            JSON_VALUE(payload, '$.robots_txt_http_status') AS INT64
        ) AS robots_txt_http_status
        , JSON_VALUE(payload, '$.robots_txt_url') AS robots_txt_url
        , JSON_VALUE(payload, '$.robots_txt_error') AS robots_txt_error
        , JSON_VALUE(payload, '$.error_type') AS error_type
        , JSON_VALUE(payload, '$.error') AS error
        , JSON_VALUE(payload, '$.content_sha256') AS content_sha256
        , JSON_VALUE(payload, '$.raw_html_path') AS raw_html_path
        , JSON_VALUE(payload, '$.normalized_text_path') AS normalized_text_path
        , JSON_VALUE(payload, '$.normalized_text_sha256') AS normalized_text_sha256
        , JSON_VALUE(payload, '$.jsonld_path') AS jsonld_path
        , JSON_VALUE(payload, '$.jsonld_sha256') AS jsonld_sha256
        , JSON_QUERY_ARRAY(payload, '$.evidence') AS evidence_json
    FROM raw_lifecycle_results
)

SELECT *
FROM renamed

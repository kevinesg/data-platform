WITH source_lifecycle_results AS (
    SELECT *
    FROM {{ ref('stg_wremotely__recheck_lifecycle_results') }}
),

ranked AS (
    SELECT
        *
        , ROW_NUMBER() OVER (
            PARTITION BY candidate_id
            ORDER BY
                CASE WHEN checked_at IS NULL THEN 1 ELSE 0 END
                , checked_at DESC
                , stage_run_id DESC
                , recheck_run_id DESC
                , source_record_index DESC
        ) AS lifecycle_recheck_rank
        , LAG(lifecycle_status) OVER (
            PARTITION BY candidate_id
            ORDER BY
                CASE WHEN checked_at IS NULL THEN 1 ELSE 0 END DESC
                , checked_at
                , stage_run_id
                , recheck_run_id
                , source_record_index
        ) AS previous_lifecycle_status
    FROM source_lifecycle_results
    WHERE candidate_id IS NOT NULL
),

final AS (
    SELECT
        candidate_id
        , url
        , source_domain
        , checked_at AS latest_lifecycle_checked_at
        , checker_version AS latest_lifecycle_checker_version
        , page_status AS latest_lifecycle_page_status
        , lifecycle_status AS latest_lifecycle_status
        , previous_lifecycle_status AS previous_lifecycle_status
        , lifecycle_signal AS latest_lifecycle_signal
        , http_status AS latest_lifecycle_http_status
        , final_url AS latest_lifecycle_final_url
        , redirect_chain_json AS latest_lifecycle_redirect_chain_json
        , content_type AS latest_lifecycle_content_type
        , attempt_count AS latest_lifecycle_attempt_count
        , extractor AS latest_lifecycle_extractor
        , robots_txt_allowed AS latest_lifecycle_robots_txt_allowed
        , robots_txt_status AS latest_lifecycle_robots_txt_status
        , robots_txt_http_status AS latest_lifecycle_robots_txt_http_status
        , robots_txt_url AS latest_lifecycle_robots_txt_url
        , robots_txt_error AS latest_lifecycle_robots_txt_error
        , error_type AS latest_lifecycle_error_type
        , error AS latest_lifecycle_error
        , content_sha256 AS latest_lifecycle_content_sha256
        , raw_html_path AS latest_lifecycle_raw_html_path
        , normalized_text_path AS latest_lifecycle_normalized_text_path
        , normalized_text_sha256 AS latest_lifecycle_normalized_text_sha256
        , jsonld_path AS latest_lifecycle_jsonld_path
        , jsonld_sha256 AS latest_lifecycle_jsonld_sha256
        , evidence_json AS latest_lifecycle_evidence_json
        , stage_run_id AS latest_lifecycle_stage_run_id
        , recheck_run_id AS latest_lifecycle_recheck_run_id
        , source_record_index AS latest_lifecycle_source_record_index
        , source_artifact_sha256 AS latest_lifecycle_artifact_sha256
    FROM ranked
    WHERE lifecycle_recheck_rank = 1
)

SELECT *
FROM final

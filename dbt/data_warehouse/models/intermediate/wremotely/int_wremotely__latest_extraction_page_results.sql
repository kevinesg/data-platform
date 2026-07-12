{{
    config(
        materialized="incremental",
        incremental_strategy="merge",
        unique_key="candidate_id"
    )
}}

WITH source_page_results AS (
    SELECT *
    FROM {{ ref('stg_wremotely__extraction_page_results') }}
),

changed_candidates AS (
    SELECT DISTINCT source.candidate_id
    FROM source_page_results AS source
    WHERE source.candidate_id IS NOT NULL
    {% if is_incremental() %}
        AND (
            source.retrieved_at > (
                SELECT COALESCE(MAX(source_updated_at), TIMESTAMP '1970-01-01 00:00:00+00')
                FROM {{ this }}
            )
            OR NOT EXISTS (
                SELECT 1
                FROM {{ this }} AS current_candidate
                WHERE current_candidate.candidate_id = source.candidate_id
            )
        )
    {% endif %}
),

ranked AS (
    SELECT
        *
        , ROW_NUMBER() OVER (
            PARTITION BY candidate_id
            ORDER BY
                CASE WHEN retrieved_at IS NULL THEN 1 ELSE 0 END
                , retrieved_at DESC
                , stage_run_id DESC
                , extraction_run_id DESC
                , source_record_index DESC
        ) AS page_result_rank
    FROM source_page_results
    INNER JOIN changed_candidates
        USING (candidate_id)
),

final AS (
    SELECT
        candidate_id
        , url
        , source_domain
        , page_status AS latest_page_status
        , retrieved_at AS latest_retrieved_at
        , http_status AS latest_http_status
        , final_url AS latest_final_url
        , redirect_chain_json AS latest_redirect_chain_json
        , content_type AS latest_content_type
        , attempt_count AS latest_attempt_count
        , extractor AS latest_extractor
        , primary_extractor AS latest_primary_extractor
        , primary_result_json AS latest_primary_result_json
        , robots_txt_allowed AS latest_robots_txt_allowed
        , robots_txt_status AS latest_robots_txt_status
        , robots_txt_http_status AS latest_robots_txt_http_status
        , robots_txt_url AS latest_robots_txt_url
        , robots_txt_error AS latest_robots_txt_error
        , robots_txt_json AS latest_robots_txt_json
        , error_type AS latest_error_type
        , error AS latest_error
        , content_sha256 AS latest_content_sha256
        , raw_html_path AS latest_raw_html_path
        , normalized_text_path AS latest_normalized_text_path
        , normalized_text_sha256 AS latest_normalized_text_sha256
        , normalized_text_char_count AS latest_normalized_text_char_count
        , jsonld_path AS latest_jsonld_path
        , jsonld_sha256 AS latest_jsonld_sha256
        , jsonld_document_count AS latest_jsonld_document_count
        , jsonld_parse_error_count AS latest_jsonld_parse_error_count
        , stage_run_id AS latest_extraction_stage_run_id
        , extraction_run_id AS latest_extraction_run_id
        , source_record_index AS latest_extraction_source_record_index
        , source_artifact_sha256 AS latest_extraction_artifact_sha256
        , retrieved_at AS source_updated_at
        , TIMESTAMP('{{ run_started_at.isoformat() }}') AS dbt_updated_at
    FROM ranked
    WHERE page_result_rank = 1
)

SELECT *
FROM final

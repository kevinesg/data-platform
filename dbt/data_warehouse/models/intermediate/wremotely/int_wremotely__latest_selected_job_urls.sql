{{
    config(
        materialized="incremental",
        incremental_strategy="merge",
        unique_key="candidate_id",
        on_schema_change="append_new_columns"
    )
}}

{% set incremental_watermark_ready = is_incremental()
    and relation_has_columns(this, ['source_updated_at', 'dbt_updated_at']) %}

WITH selected_job_urls AS (
    SELECT *
    FROM {{ ref('stg_wremotely__selected_job_urls') }}
),

selection_results AS (
    SELECT *
    FROM {{ ref('stg_wremotely__job_url_selection_results') }}
),

changed_candidates AS (
    SELECT DISTINCT source.candidate_id
    FROM selected_job_urls AS source
    WHERE source.candidate_id IS NOT NULL
    {% if incremental_watermark_ready %}
        AND (
            source.selected_at > (
                SELECT COALESCE(MAX(latest_selected_at), TIMESTAMP '1970-01-01 00:00:00+00')
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

selected_with_result_context AS (
    SELECT
        s.*
        , r.link_text AS source_link_text
        , r.link_rel AS source_link_rel
        , r.discovery_reason AS source_job_url_discovery_reason
        , r.selection_status
        , r.selection_reason
        , r.known_url_match
        , r.duplicate_url_identity
    FROM selected_job_urls AS s
    INNER JOIN changed_candidates AS changed
        USING (candidate_id)
    LEFT JOIN selection_results AS r
        ON s.selection_run_id = r.selection_run_id
            AND s.source_job_url_id = r.job_url_id
),

ranked AS (
    SELECT
        *
        , ROW_NUMBER() OVER (
            PARTITION BY candidate_id
            ORDER BY
                CASE WHEN selected_at IS NULL THEN 1 ELSE 0 END
                , selected_at DESC
                , stage_run_id DESC
                , selection_run_id DESC
                , source_record_index DESC
        ) AS selected_job_url_rank
    FROM selected_with_result_context
    WHERE candidate_id IS NOT NULL
),

final AS (
    SELECT
        candidate_id
        , url
        , normalized_url
        , source_job_url_id
        , source_candidate_id
        , source_url
        , source_domain
        , source_crawl_run_id
        , source_url_identity
        , source_type_guess
        , source_platform_guess
        , source_review_status
        , source_default_work_arrangement
        , source_link_text
        , source_link_rel
        , source_job_url_discovery_reason
        , selection_status
        , selection_reason
        , known_url_match
        , duplicate_url_identity
        , selected_at AS latest_selected_at
        , selector_version AS latest_selector_version
        , stage_run_id AS latest_selection_stage_run_id
        , selection_run_id AS latest_selection_run_id
        , source_record_index AS latest_selection_source_record_index
        , source_artifact_sha256 AS latest_selection_artifact_sha256
        , selected_at AS source_updated_at
        , TIMESTAMP('{{ run_started_at.isoformat() }}') AS dbt_updated_at
    FROM ranked
    WHERE selected_job_url_rank = 1
)

SELECT *
FROM final

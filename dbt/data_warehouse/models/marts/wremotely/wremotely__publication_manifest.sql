{{ config(alias="publication_manifest") }}

WITH serving_jobs AS (
    SELECT *
    FROM {{ ref('wremotely__serving_jobs') }}
),

snapshot AS (
    SELECT
        1 AS publication_contract_version
        , 'wremotely_serving_jobs_v1' AS serving_snapshot_contract
        , COUNT(*) AS serving_job_count
        , MAX(latest_observed_at) AS publication_watermark_at
        , TO_HEX(SHA256(COALESCE(
            STRING_AGG(serving_row_sha256, '' ORDER BY job_id)
            , ''
        ))) AS serving_snapshot_sha256
    FROM serving_jobs
),

final AS (
    SELECT
        CONCAT('wremotely-', SUBSTR(serving_snapshot_sha256, 1, 16)) AS publication_id
        , publication_contract_version
        , serving_snapshot_contract
        , serving_job_count
        , publication_watermark_at
        , serving_snapshot_sha256
        , 'dbt_modeled_not_signaled' AS publication_state
    FROM snapshot
)

SELECT *
FROM final

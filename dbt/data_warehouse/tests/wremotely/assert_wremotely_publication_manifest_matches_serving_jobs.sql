WITH serving_jobs AS (
    SELECT *
    FROM {{ ref('wremotely__serving_jobs') }}
),

expected_manifest AS (
    SELECT
        COUNT(*) AS serving_job_count
        , MAX(latest_observed_at) AS publication_watermark_at
        , TO_HEX(SHA256(COALESCE(
            STRING_AGG(serving_row_sha256, '' ORDER BY job_id)
            , ''
        ))) AS serving_snapshot_sha256
    FROM serving_jobs
),

actual_manifest AS (
    SELECT *
    FROM {{ ref('wremotely__publication_manifest') }}
)

SELECT
    a.publication_id
    , a.serving_job_count AS actual_serving_job_count
    , e.serving_job_count AS expected_serving_job_count
    , a.publication_watermark_at AS actual_publication_watermark_at
    , e.publication_watermark_at AS expected_publication_watermark_at
    , a.serving_snapshot_sha256 AS actual_serving_snapshot_sha256
    , e.serving_snapshot_sha256 AS expected_serving_snapshot_sha256
FROM actual_manifest AS a
CROSS JOIN expected_manifest AS e
WHERE a.serving_job_count != e.serving_job_count
    OR COALESCE(a.publication_watermark_at, TIMESTAMP '1970-01-01 00:00:00+00')
        != COALESCE(e.publication_watermark_at, TIMESTAMP '1970-01-01 00:00:00+00')
    OR a.serving_snapshot_sha256 != e.serving_snapshot_sha256
    OR a.publication_id != CONCAT('wremotely-', SUBSTR(e.serving_snapshot_sha256, 1, 16))

WITH serving_jobs AS (
    SELECT *
    FROM {{ ref('wremotely__serving_jobs') }}
),

companies AS (
    SELECT *
    FROM {{ ref('wremotely__companies') }}
),

expected_job_snapshot AS (
    SELECT
        COUNT(*) AS serving_job_count
        , MAX(latest_observed_at) AS job_publication_watermark_at
        , TO_HEX(SHA256(COALESCE(
            STRING_AGG(serving_row_sha256, '' ORDER BY job_id)
            , ''
        ))) AS serving_job_snapshot_sha256
    FROM serving_jobs
),

expected_company_snapshot AS (
    SELECT
        COUNT(*) AS serving_company_count
        , MAX(latest_observed_at) AS company_publication_watermark_at
        , TO_HEX(SHA256(COALESCE(
            STRING_AGG(company_row_sha256, '' ORDER BY company_id)
            , ''
        ))) AS serving_company_snapshot_sha256
    FROM companies
),

expected_manifest AS (
    SELECT
        j.serving_job_count
        , c.serving_company_count
        , (
            SELECT MAX(observed_at)
            FROM UNNEST([
                j.job_publication_watermark_at
                , c.company_publication_watermark_at
            ]) AS observed_at
        ) AS publication_watermark_at
        , j.serving_job_snapshot_sha256
        , c.serving_company_snapshot_sha256
        , TO_HEX(SHA256(TO_JSON_STRING(STRUCT(
            j.serving_job_count
            , j.serving_job_snapshot_sha256
            , c.serving_company_count
            , c.serving_company_snapshot_sha256
        )))) AS serving_snapshot_sha256
    FROM expected_job_snapshot AS j
    CROSS JOIN expected_company_snapshot AS c
),

actual_manifest AS (
    SELECT *
    FROM {{ ref('wremotely__publication_manifest') }}
)

SELECT
    a.publication_id
    , a.serving_job_count AS actual_serving_job_count
    , e.serving_job_count AS expected_serving_job_count
    , a.serving_company_count AS actual_serving_company_count
    , e.serving_company_count AS expected_serving_company_count
    , a.publication_watermark_at AS actual_publication_watermark_at
    , e.publication_watermark_at AS expected_publication_watermark_at
    , a.serving_job_snapshot_sha256 AS actual_serving_job_snapshot_sha256
    , e.serving_job_snapshot_sha256 AS expected_serving_job_snapshot_sha256
    , a.serving_company_snapshot_sha256 AS actual_serving_company_snapshot_sha256
    , e.serving_company_snapshot_sha256 AS expected_serving_company_snapshot_sha256
    , a.serving_snapshot_sha256 AS actual_serving_snapshot_sha256
    , e.serving_snapshot_sha256 AS expected_serving_snapshot_sha256
FROM actual_manifest AS a
CROSS JOIN expected_manifest AS e
WHERE a.serving_job_count != e.serving_job_count
    OR a.serving_company_count != e.serving_company_count
    OR COALESCE(a.publication_watermark_at, TIMESTAMP '1970-01-01 00:00:00+00')
        != COALESCE(e.publication_watermark_at, TIMESTAMP '1970-01-01 00:00:00+00')
    OR a.serving_job_snapshot_sha256 != e.serving_job_snapshot_sha256
    OR a.serving_company_snapshot_sha256 != e.serving_company_snapshot_sha256
    OR a.serving_snapshot_sha256 != e.serving_snapshot_sha256
    OR a.publication_id != CONCAT('wremotely-', SUBSTR(e.serving_snapshot_sha256, 1, 16))

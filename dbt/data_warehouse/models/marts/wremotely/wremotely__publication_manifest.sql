{{ config(alias="publication_manifest") }}

WITH serving_jobs AS (
    SELECT *
    FROM {{ ref('wremotely__serving_jobs') }}
),

companies AS (
    SELECT *
    FROM {{ ref('wremotely__companies') }}
),

job_snapshot AS (
    SELECT
        COUNT(*) AS serving_job_count
        , MAX(latest_observed_at) AS job_publication_watermark_at
        , TO_HEX(SHA256(COALESCE(
            STRING_AGG(serving_row_sha256, '' ORDER BY job_id)
            , ''
        ))) AS serving_job_snapshot_sha256
    FROM serving_jobs
),

company_snapshot AS (
    SELECT
        COUNT(*) AS serving_company_count
        , MAX(latest_observed_at) AS company_publication_watermark_at
        , TO_HEX(SHA256(COALESCE(
            STRING_AGG(company_row_sha256, '' ORDER BY company_id)
            , ''
        ))) AS serving_company_snapshot_sha256
    FROM companies
),

snapshot AS (
    SELECT
        2 AS publication_contract_version
        , 'wremotely_serving_snapshot_v2' AS serving_snapshot_contract
        , j.serving_job_count
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
    FROM job_snapshot AS j
    CROSS JOIN company_snapshot AS c
),

final AS (
    SELECT
        CONCAT('wremotely-', SUBSTR(serving_snapshot_sha256, 1, 16)) AS publication_id
        , publication_contract_version
        , serving_snapshot_contract
        , serving_job_count
        , serving_company_count
        , publication_watermark_at
        , serving_job_snapshot_sha256
        , serving_company_snapshot_sha256
        , serving_snapshot_sha256
        , 'dbt_modeled_not_signaled' AS publication_state
    FROM snapshot
)

SELECT *
FROM final

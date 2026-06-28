{{ config(alias="companies") }}

WITH publishable_job_facts AS (
    SELECT *
    FROM {{ ref('int_wremotely__publishable_job_facts') }}
),

company_job_facts AS (
    SELECT *
    FROM publishable_job_facts
    WHERE company_id IS NOT NULL
),

aggregated AS (
    SELECT
        company_id
        , ARRAY_AGG(
            company_name
            ORDER BY latest_observed_at DESC, job_id DESC
            LIMIT 1
        )[OFFSET(0)] AS company_name
        , ARRAY_AGG(
            company_identity_basis
            ORDER BY latest_observed_at DESC, job_id DESC
            LIMIT 1
        )[OFFSET(0)] AS company_identity_basis
        , ARRAY_AGG(
            company_identity_source_domain
            ORDER BY latest_observed_at DESC, job_id DESC
            LIMIT 1
        )[OFFSET(0)] AS source_domain
        , COUNT(*) AS open_job_count
        , MIN(source_publication_at) AS first_source_publication_at
        , MIN(latest_observed_at) AS first_observed_at
        , MAX(latest_observed_at) AS latest_observed_at
    FROM company_job_facts
    GROUP BY company_id
),

final AS (
    SELECT
        *
        , TO_HEX(SHA256(TO_JSON_STRING(STRUCT(
            company_id
            , company_name
            , company_identity_basis
            , source_domain
            , open_job_count
            , first_source_publication_at
            , first_observed_at
            , latest_observed_at
        )))) AS company_row_sha256
    FROM aggregated
)

SELECT *
FROM final

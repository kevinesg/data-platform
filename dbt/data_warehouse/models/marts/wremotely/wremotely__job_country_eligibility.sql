{{ config(alias="job_country_eligibility") }}

WITH publishable_jobs AS (
    SELECT
        job_id
        , country_eligibility_scope
    FROM {{ ref('wremotely__serving_jobs') }}
),

job_country_eligibility AS (
    SELECT *
    FROM {{ ref('int_wremotely__job_country_eligibility') }}
),

prepared AS (
    SELECT
        jce.job_id
        , jce.country_code
        , jce.eligibility_status
        , pj.country_eligibility_scope
    FROM job_country_eligibility AS jce
    INNER JOIN publishable_jobs AS pj
        ON jce.job_id = pj.job_id
),

final AS (
    SELECT
        *
        , TO_HEX(SHA256(TO_JSON_STRING(STRUCT(
            job_id
            , country_code
            , eligibility_status
            , country_eligibility_scope
        )))) AS job_country_eligibility_row_sha256
    FROM prepared
)

SELECT *
FROM final

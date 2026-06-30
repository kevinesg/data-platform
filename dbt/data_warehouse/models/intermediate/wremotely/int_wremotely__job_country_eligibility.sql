WITH candidate_country_eligibility AS (
    SELECT *
    FROM {{ ref('int_wremotely__candidate_country_eligibility') }}
),

explicit_eligible_rows AS (
    SELECT
        candidate_id AS job_id
        , country_code
        , 'ELIGIBLE' AS eligibility_status
        , validated_country_eligibility_scope AS country_eligibility_scope
    FROM candidate_country_eligibility
    CROSS JOIN UNNEST(eligible_country_codes) AS country_code
),

explicit_excluded_rows AS (
    SELECT
        candidate_id AS job_id
        , country_code
        , 'EXCLUDED' AS eligibility_status
        , validated_country_eligibility_scope AS country_eligibility_scope
    FROM candidate_country_eligibility
    CROSS JOIN UNNEST(excluded_country_codes) AS country_code
)

SELECT * FROM explicit_eligible_rows
UNION ALL
SELECT * FROM explicit_excluded_rows

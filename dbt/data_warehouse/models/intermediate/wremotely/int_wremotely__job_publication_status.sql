WITH candidate_facts AS (
    SELECT *
    FROM {{ ref('int_wremotely__current_candidate_facts') }}
),

evaluated AS (
    SELECT
        *
        , CASE
            WHEN latest_job_fact_raw_valid_through_at IS NULL THEN FALSE
            WHEN REGEXP_CONTAINS(
                COALESCE(
                    JSON_VALUE(
                        latest_job_fact_raw_valid_through_values[SAFE_OFFSET(0)]
                        , '$.value'
                    )
                    , ''
                )
                , r'^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
            )
                THEN DATE(latest_job_fact_raw_valid_through_at) < CURRENT_DATE()
            ELSE latest_job_fact_raw_valid_through_at <= CURRENT_TIMESTAMP()
        END AS has_expired_valid_through
        , COALESCE((
            latest_job_posting_type = 'JOB'
            AND latest_remote_scope IN ('REMOTE', 'HYBRID', 'ONSITE')
            AND validated_country_eligibility_scope IN ('GLOBAL', 'GLOBAL_EXCEPT', 'SPECIFIC')
            AND (
                validated_country_eligibility_scope != 'SPECIFIC'
                OR ARRAY_LENGTH(IFNULL(eligible_country_codes, ARRAY<STRING>[])) > 0
            )
            AND NULLIF(TRIM(title), '') IS NOT NULL
            AND (
                latest_job_fact_declared_language_tag IS NULL
                OR STARTS_WITH(latest_job_fact_declared_language_tag, 'en')
            )
        ), FALSE) AS meets_content_publication_requirements
        , COALESCE(
            latest_lifecycle_status = 'CLOSED'
            OR (
                latest_lifecycle_status = 'TERMINAL'
                AND previous_lifecycle_status = 'TERMINAL'
            )
            , FALSE
        ) AS has_confirmed_lifecycle_closure
    FROM candidate_facts
),

final AS (
    SELECT
        *
        , CASE
            WHEN NOT meets_content_publication_requirements THEN 'NOT_PUBLISHABLE'
            WHEN has_confirmed_lifecycle_closure THEN 'CLOSED'
            WHEN has_expired_valid_through THEN 'NOT_PUBLISHABLE'
            ELSE 'PUBLISHABLE'
        END AS publication_status
        , CASE
            WHEN latest_job_posting_type != 'JOB' OR latest_job_posting_type IS NULL
                THEN 'NOT_JOB'
            WHEN latest_remote_scope NOT IN ('REMOTE', 'HYBRID', 'ONSITE')
                OR latest_remote_scope IS NULL
                THEN 'WORK_ARRANGEMENT'
            WHEN validated_country_eligibility_scope
                NOT IN ('GLOBAL', 'GLOBAL_EXCEPT', 'SPECIFIC')
                OR validated_country_eligibility_scope IS NULL
                THEN 'COUNTRY_ELIGIBILITY_SCOPE'
            WHEN validated_country_eligibility_scope = 'SPECIFIC'
                AND ARRAY_LENGTH(IFNULL(eligible_country_codes, ARRAY<STRING>[])) = 0
                THEN 'COUNTRY_ELIGIBILITY_VALUES'
            WHEN NULLIF(TRIM(title), '') IS NULL THEN 'MISSING_TITLE'
            WHEN latest_job_fact_declared_language_tag IS NOT NULL
                AND NOT STARTS_WITH(latest_job_fact_declared_language_tag, 'en')
                THEN 'UNSUPPORTED_LANGUAGE'
            WHEN latest_lifecycle_status = 'CLOSED' THEN 'LIFECYCLE_CLOSED'
            WHEN latest_lifecycle_status = 'TERMINAL'
                AND previous_lifecycle_status = 'TERMINAL'
                THEN 'LIFECYCLE_TERMINAL_CONFIRMED'
            WHEN has_expired_valid_through THEN 'EXPIRED_VALID_THROUGH'
            ELSE 'PUBLICATION_READY'
        END AS publication_status_reason
    FROM evaluated
)

SELECT *
FROM final

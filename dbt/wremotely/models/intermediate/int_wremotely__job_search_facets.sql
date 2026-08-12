WITH publishable_jobs AS (
    SELECT *
    FROM {{ ref('int_wremotely__publishable_job_facts') }}
),

employment_source_values AS (
    SELECT
        job.job_id
        , JSON_VALUE(raw_value, '$.value') AS raw_value
    FROM publishable_jobs AS job
    CROSS JOIN UNNEST(
        COALESCE(job.raw_employment_type_values, ARRAY<JSON>[])
    ) AS raw_value

    UNION DISTINCT

    SELECT
        job_id
        , raw_employment_type AS raw_value
    FROM publishable_jobs
    WHERE NULLIF(TRIM(raw_employment_type), '') IS NOT NULL
),

normalized_employment_source_values AS (
    SELECT
        job_id
        , REGEXP_REPLACE(UPPER(TRIM(raw_value)), r'[^A-Z0-9]+', ' ')
            AS normalized_raw_value
    FROM employment_source_values
    WHERE NULLIF(TRIM(raw_value), '') IS NOT NULL
),

employment_type_matches AS (
    SELECT
        source.job_id
        , mapping.employment_type
    FROM normalized_employment_source_values AS source
    CROSS JOIN UNNEST([
        STRUCT('FULL_TIME' AS employment_type, r'(^| )FULL ?TIME($| )' AS match_pattern)
        , STRUCT('PART_TIME', r'(^| )PART ?TIME($| )')
        , STRUCT('CONTRACTOR', r'(^| )(CONTRACT|CONTRACTOR|FREELANCE)($| )')
        , STRUCT('TEMPORARY', r'(^| )(TEMP|TEMPORARY|SEASONAL)($| )')
        , STRUCT('INTERN', r'(^| )(INTERN|INTERNSHIP)($| )')
        , STRUCT('VOLUNTEER', r'(^| )VOLUNTEER($| )')
        , STRUCT('PER_DIEM', r'(^| )PER ?DIEM($| )')
    ]) AS mapping
    WHERE REGEXP_CONTAINS(source.normalized_raw_value, mapping.match_pattern)
),

employment_types AS (
    SELECT
        job_id
        , ARRAY_AGG(DISTINCT employment_type ORDER BY employment_type) AS employment_types
    FROM employment_type_matches
    GROUP BY job_id
),

search_text AS (
    SELECT
        job_id
        , REGEXP_REPLACE(
            LOWER(CONCAT(
                COALESCE(title, '')
                , ' '
                , COALESCE(company_name, '')
                , ' '
                , COALESCE(job_description, '')
            ))
            , r'[^a-z0-9]+'
            , ' '
        ) AS normalized_search_text
    FROM publishable_jobs
),

matched_search_tags AS (
    SELECT
        search_text.job_id
        , taxonomy.tag_code
    FROM search_text
    INNER JOIN {{ ref('wremotely__search_tags') }} AS taxonomy
        ON REGEXP_CONTAINS(search_text.normalized_search_text, taxonomy.match_pattern)
),

search_tags AS (
    SELECT
        job_id
        , ARRAY_AGG(DISTINCT tag_code ORDER BY tag_code) AS search_tags
    FROM matched_search_tags
    GROUP BY job_id
),

final AS (
    SELECT
        job.job_id
        , COALESCE(employment.employment_types, ARRAY<STRING>[]) AS employment_types
        , COALESCE(tags.search_tags, ARRAY<STRING>[]) AS search_tags
    FROM publishable_jobs AS job
    LEFT JOIN employment_types AS employment
        ON job.job_id = employment.job_id
    LEFT JOIN search_tags AS tags
        ON job.job_id = tags.job_id
)

SELECT *
FROM final

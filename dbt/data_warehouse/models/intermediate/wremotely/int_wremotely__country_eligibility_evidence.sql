WITH raw_evidence AS (
    SELECT *
    FROM {{ ref('stg_wremotely__country_eligibility_extractions') }}
),

countries AS (
    SELECT *
    FROM {{ ref('wremotely__countries') }}
),

country_aliases AS (
    SELECT *
    FROM {{ ref('wremotely__country_aliases') }}
),

country_match_phrases AS (
    SELECT
        country_code
        , country_name AS alias
        , REGEXP_REPLACE(LOWER(country_name), r'[^a-z0-9]+', ' ') AS alias_search_text
        , 'phrase' AS match_kind
    FROM countries

    UNION ALL

    SELECT
        country_code
        , alias
        , REGEXP_REPLACE(LOWER(alias), r'[^a-z0-9]+', ' ') AS alias_search_text
        , match_kind
    FROM country_aliases
),

country_group_match_phrases AS (
    SELECT
        country_group_code
        , alias
        , REGEXP_REPLACE(LOWER(alias), r'[^a-z0-9]+', ' ') AS alias_search_text
    FROM {{ ref('wremotely__country_group_aliases') }}
),

prepared AS (
    SELECT
        *
        , LOWER(TRIM(COALESCE(raw_value, ''))) AS normalized_raw_value
        , REGEXP_REPLACE(LOWER(COALESCE(raw_value, '')), r'[^a-z0-9]+', ' ')
            AS normalized_search_text
        , CASE
            WHEN country_field_role = 'LLM_GLOBAL_SCOPE' THEN 'GLOBAL'
            WHEN country_field_role IN (
                'LLM_EXCLUDED_COUNTRY'
                , 'LLM_EXCLUDED_GROUP'
            ) THEN 'EXCLUDED'
            WHEN country_field_role IN (
                'LLM_INCLUDED_COUNTRY'
                , 'LLM_INCLUDED_GROUP'
            ) THEN 'INCLUDED'
            WHEN country_field_role IN (
                'LLM_UNKNOWN'
                , 'LLM_INVALID_OUTPUT'
                , 'NO_COUNTRY_EVIDENCE'
            ) THEN 'UNKNOWN'
            WHEN country_field_role IN (
                'APPLICANT_LOCATION_REQUIREMENTS'
                , 'NORMALIZED_TEXT'
                , 'JOB_LOCATION'
            )
                AND COALESCE(can_restrict, TRUE)
                THEN 'INCLUDED'
            ELSE 'UNKNOWN'
        END AS evidence_direction
    FROM raw_evidence
),

global_or_unknown_evidence AS (
    SELECT
        *
        , CAST(NULL AS STRING) AS matched_country_code
        , CAST(NULL AS STRING) AS matched_country_group_code
        , CASE
            WHEN evidence_direction = 'GLOBAL' THEN 'GLOBAL_SCOPE'
            ELSE 'UNKNOWN_OR_INVALID'
        END AS match_source
    FROM prepared
    WHERE evidence_direction IN ('GLOBAL', 'UNKNOWN')
),

exact_country_code_evidence AS (
    SELECT
        p.*
        , c.country_code AS matched_country_code
        , CAST(NULL AS STRING) AS matched_country_group_code
        , 'EXACT_COUNTRY_CODE' AS match_source
    FROM prepared AS p
    INNER JOIN countries AS c
        ON TRIM(p.raw_value) IN (c.country_code, c.alpha_3_code)
    WHERE p.evidence_direction IN ('INCLUDED', 'EXCLUDED')
),

country_alias_evidence AS (
    SELECT
        p.*
        , a.country_code AS matched_country_code
        , CAST(NULL AS STRING) AS matched_country_group_code
        , 'COUNTRY_ALIAS' AS match_source
    FROM prepared AS p
    INNER JOIN country_match_phrases AS a
        ON (
            a.match_kind = 'exact_code'
            AND TRIM(p.raw_value) = a.alias
        )
        OR (
            a.match_kind = 'phrase'
            AND STRPOS(
                CONCAT(' ', p.normalized_search_text, ' ')
                , CONCAT(' ', a.alias_search_text, ' ')
            ) > 0
        )
    WHERE p.evidence_direction IN ('INCLUDED', 'EXCLUDED')
),

country_group_alias_evidence AS (
    SELECT
        p.*
        , CAST(NULL AS STRING) AS matched_country_code
        , a.country_group_code AS matched_country_group_code
        , 'COUNTRY_GROUP_ALIAS' AS match_source
    FROM prepared AS p
    INNER JOIN country_group_match_phrases AS a
        ON STRPOS(
            CONCAT(' ', p.normalized_search_text, ' ')
            , CONCAT(' ', a.alias_search_text, ' ')
        ) > 0
    WHERE p.evidence_direction IN ('INCLUDED', 'EXCLUDED')
),

combined AS (
    SELECT * FROM global_or_unknown_evidence
    UNION ALL
    SELECT * FROM exact_country_code_evidence
    UNION ALL
    SELECT * FROM country_alias_evidence
    UNION ALL
    SELECT * FROM country_group_alias_evidence
),

deduplicated AS (
    SELECT
        *
        , ROW_NUMBER() OVER (
            PARTITION BY
                candidate_id
                , source_artifact_sha256
                , source_record_index
                , evidence_direction
                , COALESCE(matched_country_code, '')
                , COALESCE(matched_country_group_code, '')
                , match_source
            ORDER BY source_evidence_index
        ) AS duplicate_rank
    FROM combined
)

SELECT * EXCEPT (duplicate_rank)
FROM deduplicated
WHERE duplicate_rank = 1

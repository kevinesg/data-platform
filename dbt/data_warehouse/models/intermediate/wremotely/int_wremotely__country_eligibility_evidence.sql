WITH raw_evidence AS (
    SELECT raw.*
    FROM {{ ref('stg_wremotely__country_eligibility_extractions') }} AS raw
    INNER JOIN {{ ref('int_wremotely__latest_classifications') }} AS latest
        ON raw.candidate_id = latest.candidate_id
        AND raw.stage_run_id = latest.latest_classification_stage_run_id
        AND raw.classification_run_id = latest.latest_classification_run_id
),

countries AS (
    SELECT *
    FROM {{ ref('wremotely__countries') }}
),

country_alias_candidates AS (
    SELECT country_code, country_name AS alias, 'phrase' AS match_kind
    FROM countries

    UNION ALL

    SELECT country_code, TRIM(SPLIT(country_name, ',')[SAFE_OFFSET(0)]) AS alias, 'phrase' AS match_kind
    FROM countries
    WHERE STRPOS(country_name, ',') > 0

    UNION ALL

    SELECT country_code, country_code AS alias, 'exact_code' AS match_kind
    FROM countries

    UNION ALL

    SELECT country_code, alpha_3_code AS alias, 'exact_code' AS match_kind
    FROM countries

    UNION ALL

    SELECT country_code, alias, match_kind
    FROM {{ ref('wremotely__country_aliases') }}
),

normalized_country_aliases AS (
    SELECT
        country_code
        , alias
        , match_kind
        , TRIM(REGEXP_REPLACE(NORMALIZE_AND_CASEFOLD(alias, NFKD), r'[^\p{L}\p{N}]+', ' '))
            AS alias_search_text
        , TRIM(REGEXP_REPLACE(NORMALIZE(alias, NFKD), r'[^A-Za-z0-9]+', ' '))
            AS alias_case_sensitive_text
    FROM country_alias_candidates
    WHERE NULLIF(TRIM(alias), '') IS NOT NULL
),

country_match_phrases AS (
    SELECT
        MIN(alias_row.country_code) AS country_code
        , alias_row.alias_search_text
        , MIN(alias_row.alias_case_sensitive_text) AS alias_case_sensitive_text
        , alias_row.match_kind
    FROM normalized_country_aliases AS alias_row
    WHERE alias_row.alias_search_text != ''
    GROUP BY alias_row.alias_search_text, alias_row.match_kind
    HAVING COUNT(DISTINCT alias_row.country_code) = 1
),

normalized_country_group_aliases AS (
    SELECT
        country_group_code
        , TRIM(REGEXP_REPLACE(NORMALIZE_AND_CASEFOLD(alias, NFKD), r'[^\p{L}\p{N}]+', ' '))
            AS alias_search_text
        , TRIM(REGEXP_REPLACE(NORMALIZE(alias, NFKD), r'[^A-Za-z0-9]+', ' '))
            AS alias_case_sensitive_text
        , REGEXP_CONTAINS(alias, r'^[A-Z0-9]{2,8}$') AS is_code
    FROM {{ ref('wremotely__country_group_aliases') }}
    WHERE NULLIF(TRIM(alias), '') IS NOT NULL
),

country_group_match_phrases AS (
    SELECT
        MIN(alias_row.country_group_code) AS country_group_code
        , alias_row.alias_search_text
        , MIN(alias_row.alias_case_sensitive_text) AS alias_case_sensitive_text
        , LOGICAL_OR(alias_row.is_code) AS is_code
    FROM normalized_country_group_aliases AS alias_row
    WHERE alias_row.alias_search_text != ''
    GROUP BY alias_row.alias_search_text
    HAVING COUNT(DISTINCT alias_row.country_group_code) = 1
),

prepared AS (
    SELECT
        *
        , CONCAT(
            stage_run_id, '|', classification_run_id, '|', CAST(source_record_index AS STRING), '|'
            , CAST(COALESCE(source_evidence_index, 0) AS STRING)
        ) AS evidence_id
        , TRIM(
            REGEXP_REPLACE(
                NORMALIZE_AND_CASEFOLD(COALESCE(raw_value, ''), NFKD)
                , r'[^\p{L}\p{N}]+'
                , ' '
            )
        ) AS normalized_raw_value
        , TRIM(
            REGEXP_REPLACE(
                NORMALIZE(COALESCE(raw_value, ''), NFKD)
                , r'[^A-Za-z0-9]+'
                , ' '
            )
        ) AS case_sensitive_search_text
        , REGEXP_REPLACE(
            LOWER(COALESCE(json_path, ''))
            , r'(\.address)?\.(addresscountry|addressregion|addresslocality|name)$'
            , ''
        ) AS location_object_path
        , CASE
            WHEN country_field_role = 'JOB_LOCATION' THEN 'UNKNOWN'
            WHEN raw_country_eligibility_scope IN ('GLOBAL', 'GLOBAL_EXCEPT')
                AND country_field_role IN (
                    'APPLICANT_LOCATION_REQUIREMENTS'
                    , 'LLM_GLOBAL_SCOPE'
                    , 'NORMALIZED_TEXT'
                    , 'SOURCE_DEFAULT_COUNTRY_ELIGIBILITY'
                )
                THEN 'GLOBAL'
            WHEN country_field_role IN ('LLM_EXCLUDED_COUNTRY', 'LLM_EXCLUDED_GROUP')
                THEN 'EXCLUDED'
            WHEN country_field_role IN ('LLM_INCLUDED_COUNTRY', 'LLM_INCLUDED_GROUP')
                THEN 'INCLUDED'
            WHEN country_field_role IN (
                'LLM_UNKNOWN', 'LLM_INVALID_OUTPUT', 'NO_COUNTRY_EVIDENCE'
            ) THEN 'UNKNOWN'
            WHEN country_field_role IN (
                'APPLICANT_LOCATION_REQUIREMENTS'
                , 'NORMALIZED_TEXT'
                , 'SOURCE_DEFAULT_COUNTRY_ELIGIBILITY'
            )
                AND COALESCE(can_restrict, TRUE)
                THEN 'INCLUDED'
            ELSE 'UNKNOWN'
        END AS evidence_direction
        , CASE
            WHEN country_field_role IN (
                'APPLICANT_LOCATION_REQUIREMENTS'
                , 'LLM_EXCLUDED_COUNTRY'
                , 'LLM_INCLUDED_COUNTRY'
                , 'SOURCE_DEFAULT_COUNTRY_ELIGIBILITY'
            ) THEN 'ATOMIC'
            WHEN country_field_role = 'NORMALIZED_TEXT' THEN 'TEXT'
            ELSE 'NONE'
        END AS country_match_mode
        , CASE
            WHEN country_field_role IN (
                'LLM_EXCLUDED_GROUP'
                , 'LLM_INCLUDED_GROUP'
                , 'SOURCE_DEFAULT_COUNTRY_ELIGIBILITY'
            ) THEN 'ATOMIC'
            WHEN country_field_role = 'NORMALIZED_TEXT' THEN 'TEXT'
            ELSE 'NONE'
        END AS country_group_match_mode
    FROM raw_evidence
),

structured_country_context AS (
    SELECT DISTINCT
        p.candidate_id
        , p.stage_run_id
        , p.classification_run_id
        , p.country_field_role AS context_role
        , p.location_object_path
        , a.country_code
    FROM prepared AS p
    INNER JOIN country_match_phrases AS a
        ON p.normalized_raw_value = a.alias_search_text
    WHERE p.country_field_role IN (
        'APPLICANT_LOCATION_REQUIREMENTS'
        , 'JOB_LOCATION'
    )
        AND REGEXP_CONTAINS(
            LOWER(COALESCE(p.json_path, ''))
            , r'\.addresscountry$'
        )
),

exact_location_alias_context_observations AS (
    SELECT DISTINCT
        a.country_code
        , a.alias_search_text
        , context.country_code AS context_country_code
        , p.candidate_id
    FROM prepared AS p
    INNER JOIN country_match_phrases AS a
        ON a.match_kind = 'phrase'
        AND p.normalized_raw_value = a.alias_search_text
    INNER JOIN structured_country_context AS context
        ON p.candidate_id = context.candidate_id
        AND p.stage_run_id = context.stage_run_id
        AND p.classification_run_id = context.classification_run_id
        AND p.country_field_role = context.context_role
        AND p.location_object_path = context.location_object_path
    WHERE p.country_field_role IN (
        'APPLICANT_LOCATION_REQUIREMENTS'
        , 'JOB_LOCATION'
    )
        AND NOT REGEXP_CONTAINS(
            LOWER(COALESCE(p.json_path, ''))
            , r'\.addresscountry$'
        )
),

observed_context_conflicting_country_aliases AS (
    SELECT
        country_code
        , alias_search_text
    FROM exact_location_alias_context_observations
    GROUP BY country_code, alias_search_text
    HAVING COUNT(DISTINCT IF(
        country_code != context_country_code, candidate_id, NULL
    )) > COUNT(DISTINCT IF(
        country_code = context_country_code, candidate_id, NULL
    ))
),

context_conflicting_country_matches AS (
    SELECT DISTINCT
        p.evidence_id
        , a.country_code
        , a.alias_search_text
    FROM prepared AS p
    INNER JOIN country_match_phrases AS a
        ON (
            p.country_match_mode = 'ATOMIC'
            AND p.normalized_raw_value = a.alias_search_text
        )
        OR (
            p.country_match_mode = 'TEXT'
            AND a.match_kind = 'phrase'
            AND STRPOS(
                CONCAT(' ', p.normalized_raw_value, ' ')
                , CONCAT(' ', a.alias_search_text, ' ')
            ) > 0
        )
    INNER JOIN observed_context_conflicting_country_aliases AS conflict
        ON a.country_code = conflict.country_code
        AND a.alias_search_text = conflict.alias_search_text
    LEFT JOIN structured_country_context AS supporting_context
        ON p.candidate_id = supporting_context.candidate_id
        AND p.stage_run_id = supporting_context.stage_run_id
        AND p.classification_run_id = supporting_context.classification_run_id
        AND a.country_code = supporting_context.country_code
        AND (
            p.country_match_mode = 'TEXT'
            OR p.location_object_path = supporting_context.location_object_path
        )
    WHERE p.evidence_direction IN ('INCLUDED', 'EXCLUDED')
        AND supporting_context.candidate_id IS NULL
),

global_or_unknown_evidence AS (
    SELECT
        *
        , CAST(NULL AS STRING) AS matched_country_code
        , CAST(NULL AS STRING) AS matched_country_group_code
        , IF(evidence_direction = 'GLOBAL', 'GLOBAL_SCOPE', 'UNKNOWN_OR_INVALID') AS match_source
    FROM prepared
    WHERE evidence_direction IN ('GLOBAL', 'UNKNOWN')
),

atomic_country_evidence AS (
    SELECT
        p.*
        , a.country_code AS matched_country_code
        , CAST(NULL AS STRING) AS matched_country_group_code
        , 'ATOMIC_COUNTRY_ALIAS' AS match_source
    FROM prepared AS p
    INNER JOIN country_match_phrases AS a
        ON p.normalized_raw_value = a.alias_search_text
    LEFT JOIN context_conflicting_country_matches AS conflict
        ON p.evidence_id = conflict.evidence_id
        AND a.country_code = conflict.country_code
        AND a.alias_search_text = conflict.alias_search_text
    WHERE p.evidence_direction IN ('INCLUDED', 'EXCLUDED')
        AND p.country_match_mode = 'ATOMIC'
        AND conflict.evidence_id IS NULL
),

country_text_match_candidates AS (
    SELECT
        p.*
        , a.country_code AS matched_country_code
        , a.alias_search_text AS matched_alias_search_text
        , CAST(NULL AS STRING) AS matched_country_group_code
        , 'COUNTRY_TEXT_ALIAS' AS match_source
    FROM prepared AS p
    INNER JOIN country_match_phrases AS a
        ON a.match_kind = 'phrase'
        AND STRPOS(
            CONCAT(' ', p.normalized_raw_value, ' ')
            , CONCAT(' ', a.alias_search_text, ' ')
        ) > 0
    LEFT JOIN context_conflicting_country_matches AS conflict
        ON p.evidence_id = conflict.evidence_id
        AND a.country_code = conflict.country_code
        AND a.alias_search_text = conflict.alias_search_text
    WHERE p.evidence_direction IN ('INCLUDED', 'EXCLUDED')
        AND p.country_match_mode = 'TEXT'
        AND conflict.evidence_id IS NULL
),

country_text_evidence AS (
    SELECT candidate.* EXCEPT (matched_alias_search_text)
    FROM country_text_match_candidates AS candidate
    LEFT JOIN country_text_match_candidates AS longer_alias
        ON longer_alias.evidence_id = candidate.evidence_id
        AND longer_alias.matched_country_code != candidate.matched_country_code
        AND LENGTH(longer_alias.matched_alias_search_text)
            > LENGTH(candidate.matched_alias_search_text)
        AND STRPOS(
            CONCAT(' ', longer_alias.matched_alias_search_text, ' ')
            , CONCAT(' ', candidate.matched_alias_search_text, ' ')
        ) > 0
    WHERE longer_alias.evidence_id IS NULL
),

atomic_country_group_evidence AS (
    SELECT
        p.*
        , CAST(NULL AS STRING) AS matched_country_code
        , a.country_group_code AS matched_country_group_code
        , 'ATOMIC_COUNTRY_GROUP_ALIAS' AS match_source
    FROM prepared AS p
    INNER JOIN country_group_match_phrases AS a
        ON p.normalized_raw_value = a.alias_search_text
    WHERE p.evidence_direction IN ('INCLUDED', 'EXCLUDED')
        AND p.country_group_match_mode = 'ATOMIC'
),

country_group_text_evidence AS (
    SELECT
        p.*
        , CAST(NULL AS STRING) AS matched_country_code
        , a.country_group_code AS matched_country_group_code
        , 'COUNTRY_GROUP_TEXT_ALIAS' AS match_source
    FROM prepared AS p
    INNER JOIN country_group_match_phrases AS a
        ON (
            NOT a.is_code
            AND STRPOS(
                CONCAT(' ', p.normalized_raw_value, ' ')
                , CONCAT(' ', a.alias_search_text, ' ')
            ) > 0
        )
        OR (
            a.is_code
            AND STRPOS(
                CONCAT(' ', p.case_sensitive_search_text, ' ')
                , CONCAT(' ', a.alias_case_sensitive_text, ' ')
            ) > 0
        )
    WHERE p.evidence_direction IN ('INCLUDED', 'EXCLUDED')
        AND p.country_group_match_mode = 'TEXT'
),

matched_restrictive_evidence AS (
    SELECT * FROM atomic_country_evidence
    UNION ALL
    SELECT * FROM country_text_evidence
    UNION ALL
    SELECT * FROM atomic_country_group_evidence
    UNION ALL
    SELECT * FROM country_group_text_evidence
),

unmatched_restrictive_evidence AS (
    SELECT
        p.*
        , CAST(NULL AS STRING) AS matched_country_code
        , CAST(NULL AS STRING) AS matched_country_group_code
        , IF(
            conflict.evidence_id IS NOT NULL
            , 'AMBIGUOUS_COUNTRY_ALIAS'
            , 'UNMATCHED_RESTRICTIVE_EVIDENCE'
        ) AS match_source
    FROM prepared AS p
    LEFT JOIN matched_restrictive_evidence AS matched
        ON matched.evidence_id = p.evidence_id
    LEFT JOIN (
        SELECT DISTINCT evidence_id
        FROM context_conflicting_country_matches
    ) AS conflict
        ON conflict.evidence_id = p.evidence_id
    WHERE p.evidence_direction IN ('INCLUDED', 'EXCLUDED')
        AND matched.evidence_id IS NULL
),

combined AS (
    SELECT * FROM global_or_unknown_evidence
    UNION ALL
    SELECT * FROM matched_restrictive_evidence
    UNION ALL
    SELECT * FROM unmatched_restrictive_evidence
),

deduplicated AS (
    SELECT
        *
        , ROW_NUMBER() OVER (
            PARTITION BY
                evidence_id
                , evidence_direction
                , COALESCE(matched_country_code, '')
                , COALESCE(matched_country_group_code, '')
            ORDER BY match_source
        ) AS duplicate_rank
    FROM combined
)

SELECT * EXCEPT (
    evidence_id
    , normalized_raw_value
    , case_sensitive_search_text
    , location_object_path
    , country_match_mode
    , country_group_match_mode
    , duplicate_rank
)
FROM deduplicated
WHERE duplicate_rank = 1

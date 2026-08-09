WITH raw_evidence AS (
    SELECT
        raw.*
        , latest.latest_remote_scope AS classification_remote_scope
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

    UNION ALL

    SELECT country_code, alias, match_kind
    FROM {{ ref('wremotely__country_cldr_aliases') }}
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

country_match_phrase_candidates AS (
    SELECT
        alias_row.*
        , MIN(alias_row.country_code) OVER (
            PARTITION BY alias_row.alias_search_text, alias_row.match_kind
        ) AS minimum_country_code
        , MAX(alias_row.country_code) OVER (
            PARTITION BY alias_row.alias_search_text, alias_row.match_kind
        ) AS maximum_country_code
        , ROW_NUMBER() OVER (
            PARTITION BY alias_row.alias_search_text, alias_row.match_kind
            ORDER BY
                alias_row.country_code
                , alias_row.alias_case_sensitive_text
                , alias_row.alias
        ) AS alias_rank
    FROM normalized_country_aliases AS alias_row
    WHERE alias_row.alias_search_text != ''
),

country_match_phrases AS (
    SELECT
        country_code
        , alias_search_text
        , alias_case_sensitive_text
        , match_kind
    FROM country_match_phrase_candidates
    WHERE minimum_country_code = maximum_country_code
        AND alias_rank = 1
),

country_text_match_phrases AS (
    SELECT country.*
    FROM country_match_phrases AS country
    WHERE country.match_kind = 'phrase'
),

country_subdivision_alias_candidates AS (
    SELECT
        subdivision_code
        , country_code
        , subdivision_name
        , subdivision_name AS alias
        , 'phrase' AS match_kind
        , TRIM(
            REGEXP_REPLACE(
                NORMALIZE_AND_CASEFOLD(subdivision_name, NFKD)
                , r'[^\p{L}\p{N}]+'
                , ' '
            )
        ) AS alias_search_text
        , TRIM(
            REGEXP_REPLACE(
                NORMALIZE(subdivision_name, NFKD)
                , r'[^A-Za-z0-9]+'
                , ' '
            )
        ) AS alias_case_sensitive_text
    FROM {{ ref('wremotely__country_subdivisions') }}
    WHERE NULLIF(TRIM(subdivision_name), '') IS NOT NULL

    UNION ALL

    SELECT
        subdivision_code
        , country_code
        , subdivision_name
        , subdivision_code AS alias
        , 'exact_code' AS match_kind
        , TRIM(
            REGEXP_REPLACE(
                NORMALIZE_AND_CASEFOLD(subdivision_code, NFKD)
                , r'[^\p{L}\p{N}]+'
                , ' '
            )
        ) AS alias_search_text
        , TRIM(
            REGEXP_REPLACE(
                NORMALIZE(subdivision_code, NFKD)
                , r'[^A-Za-z0-9]+'
                , ' '
            )
        ) AS alias_case_sensitive_text
    FROM {{ ref('wremotely__country_subdivisions') }}
    WHERE NULLIF(TRIM(subdivision_code), '') IS NOT NULL
),

subdivision_match_phrase_candidates AS (
    SELECT
        subdivision.*
        , MIN(country_code) OVER (
            PARTITION BY alias_search_text, match_kind
        ) AS minimum_country_code
        , MAX(country_code) OVER (
            PARTITION BY alias_search_text, match_kind
        ) AS maximum_country_code
        , ROW_NUMBER() OVER (
            PARTITION BY alias_search_text, match_kind
            ORDER BY country_code, subdivision_code, subdivision_name
        ) AS alias_rank
    FROM country_subdivision_alias_candidates AS subdivision
    WHERE alias_search_text != ''
),

subdivision_match_phrases AS (
    SELECT
        country_code
        , alias_search_text
        , alias_case_sensitive_text
        , match_kind
    FROM subdivision_match_phrase_candidates
    WHERE minimum_country_code = maximum_country_code
        AND alias_rank = 1
),

unambiguous_subdivision_match_phrases AS (
    SELECT subdivision.*
    FROM subdivision_match_phrases AS subdivision
    LEFT JOIN country_match_phrases AS country
        ON subdivision.alias_search_text = country.alias_search_text
        AND subdivision.match_kind = country.match_kind
    WHERE country.alias_search_text IS NULL
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

country_group_match_phrase_candidates AS (
    SELECT
        alias_row.*
        , MIN(alias_row.country_group_code) OVER (
            PARTITION BY alias_row.alias_search_text
        ) AS minimum_country_group_code
        , MAX(alias_row.country_group_code) OVER (
            PARTITION BY alias_row.alias_search_text
        ) AS maximum_country_group_code
        , MAX(CAST(alias_row.is_code AS INT64)) OVER (
            PARTITION BY alias_row.alias_search_text
        ) > 0 AS has_code_alias
        , ROW_NUMBER() OVER (
            PARTITION BY alias_row.alias_search_text
            ORDER BY
                alias_row.country_group_code
                , alias_row.alias_case_sensitive_text
        ) AS alias_rank
    FROM normalized_country_group_aliases AS alias_row
    WHERE alias_row.alias_search_text != ''
),

country_group_match_phrases AS (
    SELECT
        country_group_code
        , alias_search_text
        , alias_case_sensitive_text
        , has_code_alias AS is_code
    FROM country_group_match_phrase_candidates
    WHERE minimum_country_group_code = maximum_country_group_code
        AND alias_rank = 1
),

prepared_roles AS (
    SELECT
        *
        , country_field_role IN (
            'JOB_LOCATION'
            , 'PLATFORM_JOB_LOCATION'
            , 'BAMBOOHR_CAREERS_LIST'
            , 'BREEZY_META'
            , 'GREENHOUSE_REMIX'
            , 'JAZZHR_VISIBLE_HTML'
            , 'JOBVITE_PRELOADED_DATA'
            , 'NEXTJS'
            , 'PERSONIO_VISIBLE_HTML'
            , 'SMARTRECRUITERS_MICRODATA'
            , 'WORKABLE_WIDGET'
        ) AS is_location_evidence_role
        , (
            country_field_role = 'PLATFORM_JOB_LOCATION'
            AND LOWER(COALESCE(source_platform_guess, '')) IN (
                'ashby'
                , 'bamboohr'
                , 'breezy'
                , 'greenhouse'
                , 'jazzhr'
                , 'jobvite'
                , 'lever'
                , 'personio'
                , 'rippling'
                , 'smartrecruiters'
                , 'workable'
            )
        ) OR (
            country_field_role = country_field_source_system
            AND country_field_role IN (
                'BAMBOOHR_CAREERS_LIST'
                , 'BREEZY_META'
                , 'GREENHOUSE_REMIX'
                , 'JAZZHR_VISIBLE_HTML'
                , 'JOBVITE_PRELOADED_DATA'
                , 'NEXTJS'
                , 'PERSONIO_VISIBLE_HTML'
                , 'SMARTRECRUITERS_MICRODATA'
                , 'WORKABLE_WIDGET'
            )
        ) AS is_reviewed_platform_location_role
    FROM raw_evidence
),

prepared_inputs AS (
    SELECT
        *
        , raw_country_eligibility_scope NOT IN ('GLOBAL', 'GLOBAL_EXCEPT')
            AND (
                (
                    country_field_role = 'JOB_LOCATION'
                    AND classification_remote_scope = 'ONSITE'
                )
                OR (
                    COALESCE(can_restrict, FALSE)
                    AND (
                        is_reviewed_platform_location_role
                        OR (
                            country_field_role = 'JOB_LOCATION'
                            AND (
                                (
                                    classification_remote_scope IN ('REMOTE', 'HYBRID')
                                    AND LOWER(COALESCE(source_platform_guess, '')) = 'lever'
                                )
                                OR (
                                    classification_remote_scope IN ('REMOTE', 'HYBRID')
                                    AND LOWER(COALESCE(source_platform_guess, '')) = 'workday'
                                )
                            )
                        )
                    )
                )
            ) AS is_restricting_location_evidence
    FROM prepared_roles
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
            WHEN is_restricting_location_evidence THEN 'INCLUDED'
            WHEN is_location_evidence_role THEN 'UNKNOWN'
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
            WHEN is_restricting_location_evidence
                AND country_field_role = 'JOB_LOCATION'
                AND REGEXP_CONTAINS(LOWER(COALESCE(json_path, '')), r'\.addresscountry$')
                THEN 'ATOMIC'
            WHEN is_restricting_location_evidence THEN 'TEXT'
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
            WHEN is_restricting_location_evidence
                AND country_field_role = 'JOB_LOCATION'
                AND REGEXP_CONTAINS(LOWER(COALESCE(json_path, '')), r'\.addresscountry$')
                THEN 'ATOMIC'
            WHEN is_restricting_location_evidence THEN 'TEXT'
            WHEN country_field_role IN (
                'APPLICANT_LOCATION_REQUIREMENTS'
                , 'LLM_EXCLUDED_GROUP'
                , 'LLM_INCLUDED_GROUP'
                , 'SOURCE_DEFAULT_COUNTRY_ELIGIBILITY'
            ) THEN 'ATOMIC'
            WHEN country_field_role = 'NORMALIZED_TEXT' THEN 'TEXT'
            ELSE 'NONE'
        END AS country_group_match_mode
    FROM prepared_inputs
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

location_alias_context_observations AS (
    SELECT DISTINCT
        a.country_code
        , a.alias_search_text
        , context.country_code AS context_country_code
        , p.candidate_id
    FROM prepared AS p
    INNER JOIN country_match_phrases AS a
        ON a.match_kind = 'phrase'
        AND STRPOS(
            CONCAT(' ', p.normalized_raw_value, ' ')
            , CONCAT(' ', a.alias_search_text, ' ')
        ) > 0
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
    FROM location_alias_context_observations
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
        , a.alias_case_sensitive_text AS matched_alias_case_sensitive_text
        , a.match_kind AS matched_alias_kind
        , ARRAY_LENGTH(
            SPLIT(
                IF(
                    a.match_kind = 'exact_code'
                    , CONCAT(' ', p.case_sensitive_search_text, ' ')
                    , CONCAT(' ', p.normalized_raw_value, ' ')
                )
                , CONCAT(
                    ' '
                    , IF(
                        a.match_kind = 'exact_code'
                        , a.alias_case_sensitive_text
                        , a.alias_search_text
                    )
                    , ' '
                )
            )
        ) - 1 AS matched_alias_occurrence_count
        , CAST(NULL AS STRING) AS matched_country_group_code
        , 'COUNTRY_TEXT_ALIAS' AS match_source
    FROM prepared AS p
    INNER JOIN country_text_match_phrases AS a
        ON STRPOS(
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

subdivision_text_match_candidates AS (
    SELECT
        p.*
        , subdivision.country_code AS matched_country_code
        , subdivision.alias_search_text AS matched_alias_search_text
        , subdivision.alias_case_sensitive_text AS matched_alias_case_sensitive_text
        , subdivision.match_kind AS matched_alias_kind
        , ARRAY_LENGTH(
            SPLIT(
                CONCAT(' ', p.case_sensitive_search_text, ' ')
                , CONCAT(' ', subdivision.alias_case_sensitive_text, ' ')
            )
        ) - 1 AS matched_alias_occurrence_count
        , CAST(NULL AS STRING) AS matched_country_group_code
        , 'COUNTRY_SUBDIVISION_TEXT_ALIAS' AS match_source
    FROM prepared AS p
    INNER JOIN unambiguous_subdivision_match_phrases AS subdivision
        ON (
            subdivision.match_kind = 'phrase'
            AND STRPOS(
                CONCAT(' ', p.case_sensitive_search_text, ' ')
                , CONCAT(' ', subdivision.alias_case_sensitive_text, ' ')
            ) > 0
        )
        OR (
            subdivision.match_kind = 'exact_code'
            AND REGEXP_CONTAINS(
                NORMALIZE(COALESCE(p.raw_value, ''), NFKD)
                , CONCAT(
                    r'(^|[^A-Za-z0-9])'
                    , REPLACE(
                        subdivision.alias_case_sensitive_text
                        , ' '
                        , '-'
                    )
                    , r'([^A-Za-z0-9]|$)'
                )
            )
        )
    WHERE p.evidence_direction IN ('INCLUDED', 'EXCLUDED')
        AND p.country_match_mode = 'TEXT'
),

subdivision_country_context AS (
    SELECT
        subdivision.evidence_id
        , subdivision.matched_country_code
        , subdivision.matched_alias_search_text
        , subdivision.matched_alias_kind
        , COUNTIF(
            country.matched_alias_kind = 'phrase'
            AND country.matched_country_code = subdivision.matched_country_code
        ) > 0 AS has_parent_country_context
        , COUNTIF(
            country.matched_alias_kind = 'phrase'
            AND country.matched_country_code != subdivision.matched_country_code
            AND LENGTH(subdivision.matched_alias_search_text)
                > LENGTH(country.matched_alias_search_text)
            AND STRPOS(
                CONCAT(' ', subdivision.matched_alias_search_text, ' ')
                , CONCAT(' ', country.matched_alias_search_text, ' ')
            ) > 0
        ) > 0 AS contains_country_name
    FROM subdivision_text_match_candidates AS subdivision
    LEFT JOIN country_text_match_candidates AS country
        ON country.evidence_id = subdivision.evidence_id
    GROUP BY
        subdivision.evidence_id
        , subdivision.matched_country_code
        , subdivision.matched_alias_search_text
        , subdivision.matched_alias_kind
),

subdivision_text_matches AS (
    SELECT candidate.*
    FROM subdivision_text_match_candidates AS candidate
    INNER JOIN subdivision_country_context AS context
        ON context.evidence_id = candidate.evidence_id
        AND context.matched_country_code = candidate.matched_country_code
        AND context.matched_alias_search_text = candidate.matched_alias_search_text
        AND context.matched_alias_kind = candidate.matched_alias_kind
    LEFT JOIN subdivision_text_match_candidates AS longer_alias
        ON longer_alias.evidence_id = candidate.evidence_id
        AND longer_alias.matched_country_code != candidate.matched_country_code
        AND LENGTH(longer_alias.matched_alias_search_text)
            > LENGTH(candidate.matched_alias_search_text)
        AND STRPOS(
            CONCAT(' ', longer_alias.matched_alias_search_text, ' ')
            , CONCAT(' ', candidate.matched_alias_search_text, ' ')
        ) > 0
    WHERE longer_alias.evidence_id IS NULL
        AND (
            candidate.matched_alias_kind = 'exact_code'
            OR context.has_parent_country_context
            OR context.contains_country_name
        )
),

country_subdivision_conflicts AS (
    SELECT
        country.evidence_id
        , country.matched_country_code
        , country.matched_alias_search_text
        , country.matched_alias_occurrence_count
        , SUM(subdivision.matched_alias_occurrence_count)
            AS conflicting_subdivision_occurrence_count
    FROM country_text_match_candidates AS country
    INNER JOIN subdivision_text_matches AS subdivision
        ON subdivision.evidence_id = country.evidence_id
        AND subdivision.matched_country_code != country.matched_country_code
        AND LENGTH(subdivision.matched_alias_search_text)
            > LENGTH(country.matched_alias_search_text)
        AND STRPOS(
            CONCAT(' ', subdivision.matched_alias_search_text, ' ')
            , CONCAT(' ', country.matched_alias_search_text, ' ')
        ) > 0
    GROUP BY
        country.evidence_id
        , country.matched_country_code
        , country.matched_alias_search_text
        , country.matched_alias_occurrence_count
),

country_text_evidence AS (
    SELECT candidate.* EXCEPT (
        matched_alias_search_text
        , matched_alias_case_sensitive_text
        , matched_alias_kind
        , matched_alias_occurrence_count
    )
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
    LEFT JOIN country_subdivision_conflicts AS subdivision_conflict
        ON subdivision_conflict.evidence_id = candidate.evidence_id
        AND subdivision_conflict.matched_country_code = candidate.matched_country_code
        AND subdivision_conflict.matched_alias_search_text
            = candidate.matched_alias_search_text
    WHERE longer_alias.evidence_id IS NULL
        AND COALESCE(
            subdivision_conflict.conflicting_subdivision_occurrence_count
            , 0
        ) < candidate.matched_alias_occurrence_count
),

subdivision_text_evidence AS (
    SELECT * EXCEPT (
        matched_alias_search_text
        , matched_alias_case_sensitive_text
        , matched_alias_kind
        , matched_alias_occurrence_count
    )
    FROM subdivision_text_matches
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
    SELECT * FROM subdivision_text_evidence
    UNION ALL
    SELECT * FROM atomic_country_group_evidence
    UNION ALL
    SELECT * FROM country_group_text_evidence
),

unmatched_location_label_evidence AS (
    SELECT
        p.* REPLACE ('UNKNOWN' AS evidence_direction)
        , CAST(NULL AS STRING) AS matched_country_code
        , CAST(NULL AS STRING) AS matched_country_group_code
        , 'UNMATCHED_LOCATION_LABEL' AS match_source
    FROM prepared AS p
    LEFT JOIN matched_restrictive_evidence AS matched
        ON matched.evidence_id = p.evidence_id
    LEFT JOIN (
        SELECT DISTINCT evidence_id
        FROM context_conflicting_country_matches
    ) AS conflict
        ON conflict.evidence_id = p.evidence_id
    WHERE p.evidence_direction IN ('INCLUDED', 'EXCLUDED')
        AND p.rule = 'location_shaped_remote_label'
        AND matched.evidence_id IS NULL
        AND conflict.evidence_id IS NULL
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
        AND NOT (
            COALESCE(p.rule, '') = 'location_shaped_remote_label'
            AND conflict.evidence_id IS NULL
        )
),

combined AS (
    SELECT * FROM global_or_unknown_evidence
    UNION ALL
    SELECT * FROM matched_restrictive_evidence
    UNION ALL
    SELECT * FROM unmatched_location_label_evidence
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
    , is_location_evidence_role
    , is_reviewed_platform_location_role
    , is_restricting_location_evidence
    , duplicate_rank
)
FROM deduplicated
WHERE duplicate_rank = 1

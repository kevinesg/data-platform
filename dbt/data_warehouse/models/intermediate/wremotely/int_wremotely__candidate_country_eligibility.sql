WITH evidence AS (
    SELECT *
    FROM {{ ref('int_wremotely__country_eligibility_evidence') }}
),

group_memberships AS (
    SELECT *
    FROM {{ ref('wremotely__country_group_memberships') }}
),

country_direction_rows AS (
    SELECT
        candidate_id
        , evidence_direction
        , matched_country_code AS country_code
    FROM evidence
    WHERE matched_country_code IS NOT NULL

    UNION ALL

    SELECT
        e.candidate_id
        , e.evidence_direction
        , gm.country_code
    FROM evidence AS e
    INNER JOIN group_memberships AS gm
        ON e.matched_country_group_code = gm.country_group_code
    WHERE e.matched_country_group_code IS NOT NULL
),

country_rollup AS (
    SELECT
        candidate_id
        , ARRAY_AGG(DISTINCT country_code IGNORE NULLS ORDER BY country_code) AS country_codes
        , ARRAY_AGG(
            DISTINCT IF(evidence_direction = 'INCLUDED', country_code, NULL)
            IGNORE NULLS
            ORDER BY IF(evidence_direction = 'INCLUDED', country_code, NULL)
        ) AS included_country_codes
        , ARRAY_AGG(
            DISTINCT IF(evidence_direction = 'EXCLUDED', country_code, NULL)
            IGNORE NULLS
            ORDER BY IF(evidence_direction = 'EXCLUDED', country_code, NULL)
        ) AS excluded_country_codes
    FROM country_direction_rows
    GROUP BY candidate_id
),

group_rollup AS (
    SELECT
        candidate_id
        , ARRAY_AGG(
            DISTINCT IF(evidence_direction = 'INCLUDED', matched_country_group_code, NULL)
            IGNORE NULLS
            ORDER BY IF(evidence_direction = 'INCLUDED', matched_country_group_code, NULL)
        ) AS included_country_group_codes
        , ARRAY_AGG(
            DISTINCT IF(evidence_direction = 'EXCLUDED', matched_country_group_code, NULL)
            IGNORE NULLS
            ORDER BY IF(evidence_direction = 'EXCLUDED', matched_country_group_code, NULL)
        ) AS excluded_country_group_codes
    FROM evidence
    WHERE matched_country_group_code IS NOT NULL
    GROUP BY candidate_id
),

evidence_rollup AS (
    SELECT
        candidate_id
        , MAX(IF(evidence_direction = 'GLOBAL', TRUE, FALSE)) AS has_global_evidence
        , MAX(IF(evidence_direction = 'UNKNOWN', TRUE, FALSE)) AS has_unknown_evidence
        , COUNT(*) AS country_eligibility_evidence_count
        , COUNTIF(matched_country_code IS NOT NULL) AS matched_country_evidence_count
        , COUNTIF(matched_country_group_code IS NOT NULL) AS matched_country_group_evidence_count
    FROM evidence
    GROUP BY candidate_id
),

combined AS (
    SELECT
        er.candidate_id
        , CASE
            WHEN er.has_global_evidence
                AND ARRAY_LENGTH(IFNULL(cr.excluded_country_codes, ARRAY<STRING>[])) > 0
                THEN 'GLOBAL_EXCEPT'
            WHEN er.has_global_evidence THEN 'GLOBAL'
            WHEN ARRAY_LENGTH(IFNULL(cr.included_country_codes, ARRAY<STRING>[])) > 0 THEN 'SPECIFIC'
            ELSE 'UNKNOWN'
        END AS validated_country_eligibility_scope
        , ARRAY(
            SELECT country_code
            FROM UNNEST(IFNULL(cr.included_country_codes, ARRAY<STRING>[])) AS country_code
            WHERE country_code NOT IN UNNEST(IFNULL(cr.excluded_country_codes, ARRAY<STRING>[]))
            ORDER BY country_code
        ) AS eligible_country_codes
        , IFNULL(cr.excluded_country_codes, ARRAY<STRING>[]) AS excluded_country_codes
        , IFNULL(gr.included_country_group_codes, ARRAY<STRING>[]) AS included_country_group_codes
        , IFNULL(gr.excluded_country_group_codes, ARRAY<STRING>[]) AS excluded_country_group_codes
        , er.has_global_evidence
        , er.has_unknown_evidence
        , er.country_eligibility_evidence_count
        , er.matched_country_evidence_count
        , er.matched_country_group_evidence_count
    FROM evidence_rollup AS er
    LEFT JOIN country_rollup AS cr
        ON er.candidate_id = cr.candidate_id
    LEFT JOIN group_rollup AS gr
        ON er.candidate_id = gr.candidate_id
)

SELECT *
FROM combined

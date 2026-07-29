WITH current_candidate_facts AS (
    SELECT *
    FROM {{ ref('int_wremotely__current_candidate_facts') }}
),

candidate_country_eligibility AS (
    SELECT *
    FROM {{ ref('int_wremotely__candidate_country_eligibility') }}
)

SELECT
    COALESCE(cf.candidate_id, ce.candidate_id) AS candidate_id
FROM current_candidate_facts AS cf
FULL OUTER JOIN candidate_country_eligibility AS ce
    USING (candidate_id)
WHERE cf.candidate_id IS NULL
    OR TO_JSON_STRING(STRUCT(
        cf.validated_country_eligibility_scope AS validated_country_eligibility_scope
        , cf.eligible_country_codes AS eligible_country_codes
        , cf.excluded_country_codes AS excluded_country_codes
        , cf.included_country_group_codes AS included_country_group_codes
        , cf.excluded_country_group_codes AS excluded_country_group_codes
        , cf.has_global_evidence AS has_global_evidence
        , cf.has_unknown_evidence AS has_unknown_evidence
        , cf.country_eligibility_evidence_count AS country_eligibility_evidence_count
        , cf.matched_country_evidence_count AS matched_country_evidence_count
        , cf.matched_country_group_evidence_count AS matched_country_group_evidence_count
        , cf.has_country_eligibility_evidence AS has_country_eligibility_evidence
    )) IS DISTINCT FROM TO_JSON_STRING(STRUCT(
        ce.validated_country_eligibility_scope AS validated_country_eligibility_scope
        , IFNULL(ce.eligible_country_codes, ARRAY<STRING>[]) AS eligible_country_codes
        , IFNULL(ce.excluded_country_codes, ARRAY<STRING>[]) AS excluded_country_codes
        , IFNULL(ce.included_country_group_codes, ARRAY<STRING>[])
            AS included_country_group_codes
        , IFNULL(ce.excluded_country_group_codes, ARRAY<STRING>[])
            AS excluded_country_group_codes
        , ce.has_global_evidence AS has_global_evidence
        , ce.has_unknown_evidence AS has_unknown_evidence
        , ce.country_eligibility_evidence_count AS country_eligibility_evidence_count
        , ce.matched_country_evidence_count AS matched_country_evidence_count
        , ce.matched_country_group_evidence_count AS matched_country_group_evidence_count
        , ce.candidate_id IS NOT NULL AS has_country_eligibility_evidence
    ))

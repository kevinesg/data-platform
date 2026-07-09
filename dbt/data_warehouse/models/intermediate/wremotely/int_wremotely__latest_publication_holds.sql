WITH publication_holds AS (
    SELECT *
    FROM {{ ref('stg_wremotely__publication_holds') }}
),

ranked AS (
    SELECT
        *
        , ROW_NUMBER() OVER (
            PARTITION BY candidate_id
            ORDER BY
                CASE WHEN evaluated_at IS NULL THEN 1 ELSE 0 END
                , evaluated_at DESC
                , stage_run_id DESC
                , publication_hold_run_id DESC
                , source_record_index DESC
        ) AS publication_hold_rank
    FROM publication_holds
    WHERE candidate_id IS NOT NULL
),

final AS (
    SELECT
        candidate_id
        , url
        , evaluated_at AS latest_publication_hold_evaluated_at
        , hold_status AS latest_publication_hold_status
        , hold_reason_code AS latest_publication_hold_reason_code
        , hold_guardrail_reason AS latest_publication_hold_guardrail_reason
        , hold_evidence_quote AS latest_publication_hold_evidence_quote
        , classification_remote_scope AS latest_publication_hold_classification_remote_scope
        , classification_country_eligibility_scope
            AS latest_publication_hold_classification_country_eligibility_scope
        , classification_serving_decision
            AS latest_publication_hold_classification_serving_decision
        , declared_language_raw AS latest_publication_hold_declared_language_raw
        , declared_language_tag AS latest_publication_hold_declared_language_tag
        , declared_language_source AS latest_publication_hold_declared_language_source
        , source_candidate_id AS latest_publication_hold_source_candidate_id
        , source_url AS latest_publication_hold_source_url
        , source_url_identity AS latest_publication_hold_source_url_identity
        , source_type_guess AS latest_publication_hold_source_type_guess
        , source_platform_guess AS latest_publication_hold_source_platform_guess
        , source_review_status AS latest_publication_hold_source_review_status
        , source_default_work_arrangement
            AS latest_publication_hold_source_default_work_arrangement
        , source_content_sha256 AS latest_publication_hold_source_content_sha256
        , normalized_text_sha256 AS latest_publication_hold_normalized_text_sha256
        , jsonld_sha256 AS latest_publication_hold_jsonld_sha256
        , policy_sha256 AS latest_publication_hold_policy_sha256
        , publication_hold_evaluator_version
            AS latest_publication_hold_evaluator_version
        , llm_response_sha256 AS latest_publication_hold_llm_response_sha256
        , llm_skipped_reason AS latest_publication_hold_llm_skipped_reason
        , stage_run_id AS latest_publication_hold_stage_run_id
        , publication_hold_run_id AS latest_publication_hold_run_id
        , source_record_index AS latest_publication_hold_source_record_index
        , source_artifact_sha256 AS latest_publication_hold_artifact_sha256
    FROM ranked
    WHERE publication_hold_rank = 1
)

SELECT *
FROM final

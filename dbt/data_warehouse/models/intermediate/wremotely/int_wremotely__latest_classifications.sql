WITH source_classifications AS (
    SELECT *
    FROM {{ ref('stg_wremotely__classification_classifications') }}
),

ranked AS (
    SELECT
        *
        , ROW_NUMBER() OVER (
            PARTITION BY candidate_id
            ORDER BY
                CASE WHEN classified_at IS NULL THEN 1 ELSE 0 END
                , classified_at DESC
                , stage_run_id DESC
                , classification_run_id DESC
                , source_record_index DESC
        ) AS classification_rank
    FROM source_classifications
    WHERE candidate_id IS NOT NULL
),

final AS (
    SELECT
        candidate_id
        , url
        , classified_at AS latest_classified_at
        , classifier_version AS latest_classifier_version
        , model AS latest_classifier_model
        , classification_status AS latest_classification_status
        , job_posting_type AS latest_job_posting_type
        , job_status AS latest_job_status
        , remote_scope AS latest_remote_scope
        , country_eligibility_scope AS latest_country_eligibility_scope
        , target_country AS latest_target_country
        , target_country_code AS latest_target_country_code
        , target_country_eligibility AS latest_target_country_eligibility
        , serving_decision AS latest_serving_decision
        , source_candidate_id AS latest_classification_source_candidate_id
        , source_url AS latest_classification_source_url
        , source_url_identity AS latest_classification_source_url_identity
        , source_type_guess AS latest_classification_source_type_guess
        , source_platform_guess AS latest_classification_source_platform_guess
        , source_review_status AS latest_classification_source_review_status
        , source_default_work_arrangement AS latest_classification_source_default_work_arrangement
        , source_content_sha256 AS latest_classification_source_content_sha256
        , normalized_text_sha256 AS latest_classification_normalized_text_sha256
        , jsonld_sha256 AS latest_classification_jsonld_sha256
        , declared_language_raw AS latest_classification_declared_language_raw
        , declared_language_tag AS latest_classification_declared_language_tag
        , declared_language_source AS latest_classification_declared_language_source
        , evidence_json AS latest_classification_evidence_json
        , stage_run_id AS latest_classification_stage_run_id
        , classification_run_id AS latest_classification_run_id
        , source_record_index AS latest_classification_source_record_index
        , source_artifact_sha256 AS latest_classification_artifact_sha256
    FROM ranked
    WHERE classification_rank = 1
)

SELECT *
FROM final

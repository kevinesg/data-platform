WITH candidates AS (
    SELECT *
    FROM {{ ref('int_wremotely__latest_discovery_candidates') }}
),

extractions AS (
    SELECT *
    FROM {{ ref('int_wremotely__latest_extraction_page_results') }}
),

classifications AS (
    SELECT *
    FROM {{ ref('int_wremotely__latest_classifications') }}
),

lifecycle_rechecks AS (
    SELECT *
    FROM {{ ref('int_wremotely__latest_lifecycle_rechecks') }}
),

country_eligibility AS (
    SELECT *
    FROM {{ ref('int_wremotely__candidate_country_eligibility') }}
),

final AS (
    SELECT
        c.candidate_id
        , c.url
        , c.title
        , c.company_name
        , c.candidate_required_location
        , c.publication_at
        , c.attribution_name
        , c.attribution_url
        , c.snippet
        , c.discoveries_json
        , c.latest_discovery_stage_run_id
        , c.latest_discovery_run_id
        , c.latest_discovery_source_record_index
        , c.latest_discovery_artifact_sha256
        , e.source_domain
        , e.latest_page_status
        , e.latest_retrieved_at
        , e.latest_http_status
        , e.latest_final_url
        , e.latest_redirect_chain_json
        , e.latest_content_type
        , e.latest_attempt_count
        , e.latest_extractor
        , e.latest_robots_txt_allowed
        , e.latest_robots_txt_status
        , e.latest_robots_txt_http_status
        , e.latest_robots_txt_url
        , e.latest_robots_txt_error
        , e.latest_robots_txt_json
        , e.latest_error_type
        , e.latest_error
        , e.latest_content_sha256
        , e.latest_raw_html_path
        , e.latest_normalized_text_path
        , e.latest_normalized_text_sha256
        , e.latest_jsonld_path
        , e.latest_jsonld_sha256
        , e.latest_jsonld_document_count
        , e.latest_jsonld_parse_error_count
        , e.latest_extraction_stage_run_id
        , e.latest_extraction_run_id
        , e.latest_extraction_source_record_index
        , e.latest_extraction_artifact_sha256
        , cl.latest_classified_at
        , cl.latest_classifier_version
        , cl.latest_classifier_model
        , cl.latest_classification_status
        , cl.latest_job_posting_type
        , cl.latest_job_status
        , cl.latest_remote_scope
        , cl.latest_country_eligibility_scope
        , cl.latest_target_country
        , cl.latest_target_country_code
        , cl.latest_target_country_eligibility
        , cl.latest_serving_decision
        , cl.latest_classification_source_content_sha256
        , cl.latest_classification_normalized_text_sha256
        , cl.latest_classification_jsonld_sha256
        , cl.latest_classification_evidence_json
        , cl.latest_classification_stage_run_id
        , cl.latest_classification_run_id
        , cl.latest_classification_source_record_index
        , cl.latest_classification_artifact_sha256
        , lr.latest_lifecycle_checked_at
        , lr.latest_lifecycle_checker_version
        , lr.latest_lifecycle_page_status
        , lr.latest_lifecycle_status
        , lr.latest_lifecycle_signal
        , lr.latest_lifecycle_http_status
        , lr.latest_lifecycle_final_url
        , lr.latest_lifecycle_redirect_chain_json
        , lr.latest_lifecycle_content_type
        , lr.latest_lifecycle_attempt_count
        , lr.latest_lifecycle_extractor
        , lr.latest_lifecycle_robots_txt_allowed
        , lr.latest_lifecycle_robots_txt_status
        , lr.latest_lifecycle_robots_txt_http_status
        , lr.latest_lifecycle_robots_txt_url
        , lr.latest_lifecycle_robots_txt_error
        , lr.latest_lifecycle_error_type
        , lr.latest_lifecycle_error
        , lr.latest_lifecycle_content_sha256
        , lr.latest_lifecycle_raw_html_path
        , lr.latest_lifecycle_normalized_text_path
        , lr.latest_lifecycle_normalized_text_sha256
        , lr.latest_lifecycle_jsonld_path
        , lr.latest_lifecycle_jsonld_sha256
        , lr.latest_lifecycle_evidence_json
        , lr.latest_lifecycle_stage_run_id
        , lr.latest_lifecycle_recheck_run_id
        , lr.latest_lifecycle_source_record_index
        , lr.latest_lifecycle_artifact_sha256
        , ce.validated_country_eligibility_scope
        , ce.eligible_country_codes
        , ce.excluded_country_codes
        , ce.included_country_group_codes
        , ce.excluded_country_group_codes
        , ce.has_global_evidence
        , ce.has_unknown_evidence
        , ce.country_eligibility_evidence_count
        , ce.matched_country_evidence_count
        , ce.matched_country_group_evidence_count
        , e.candidate_id IS NOT NULL AS has_extraction
        , cl.candidate_id IS NOT NULL AS has_classification
        , lr.candidate_id IS NOT NULL AS has_lifecycle_recheck
        , ce.candidate_id IS NOT NULL AS has_country_eligibility_evidence
        , (
            SELECT MAX(observed_at)
            FROM UNNEST([
                c.publication_at
                , e.latest_retrieved_at
                , cl.latest_classified_at
                , lr.latest_lifecycle_checked_at
            ]) AS observed_at
        ) AS latest_observed_at
    FROM candidates AS c
    LEFT JOIN extractions AS e
        ON c.candidate_id = e.candidate_id
    LEFT JOIN classifications AS cl
        ON c.candidate_id = cl.candidate_id
    LEFT JOIN lifecycle_rechecks AS lr
        ON c.candidate_id = lr.candidate_id
    LEFT JOIN country_eligibility AS ce
        ON c.candidate_id = ce.candidate_id
)

SELECT *
FROM final

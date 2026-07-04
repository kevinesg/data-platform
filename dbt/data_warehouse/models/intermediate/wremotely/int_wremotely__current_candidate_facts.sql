WITH discovery_candidates AS (
    SELECT *
    FROM {{ ref('int_wremotely__latest_discovery_candidates') }}
),

selected_job_urls AS (
    SELECT *
    FROM {{ ref('int_wremotely__latest_selected_job_urls') }}
),

candidate_keys AS (
    SELECT candidate_id
    FROM discovery_candidates

    UNION DISTINCT

    SELECT candidate_id
    FROM selected_job_urls
),

candidate_base AS (
    SELECT
        k.candidate_id
        , COALESCE(s.url, c.url) AS url
        , COALESCE(NULLIF(TRIM(c.title), ''), NULLIF(TRIM(s.source_link_text), '')) AS title
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
        , s.normalized_url AS selected_normalized_url
        , s.source_job_url_id AS selected_source_job_url_id
        , s.source_candidate_id AS selected_source_candidate_id
        , s.source_url AS selected_source_url
        , s.source_domain AS selected_source_domain
        , s.source_crawl_run_id AS selected_source_crawl_run_id
        , s.source_url_identity AS selected_source_url_identity
        , s.source_type_guess AS selected_source_type_guess
        , s.source_platform_guess AS selected_source_platform_guess
        , s.source_review_status AS selected_source_review_status
        , s.source_default_work_arrangement AS selected_source_default_work_arrangement
        , s.source_link_text AS selected_source_link_text
        , s.source_link_rel AS selected_source_link_rel
        , s.source_job_url_discovery_reason AS selected_source_job_url_discovery_reason
        , s.selection_status
        , s.selection_reason
        , s.known_url_match
        , s.duplicate_url_identity
        , s.latest_selected_at
        , s.latest_selector_version
        , s.latest_selection_stage_run_id
        , s.latest_selection_run_id
        , s.latest_selection_source_record_index
        , s.latest_selection_artifact_sha256
    FROM candidate_keys AS k
    LEFT JOIN discovery_candidates AS c
        ON k.candidate_id = c.candidate_id
    LEFT JOIN selected_job_urls AS s
        ON k.candidate_id = s.candidate_id
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
        , c.selected_normalized_url
        , c.selected_source_job_url_id
        , c.selected_source_candidate_id
        , c.selected_source_url
        , c.selected_source_domain
        , c.selected_source_crawl_run_id
        , c.selected_source_url_identity
        , c.selected_source_type_guess
        , c.selected_source_platform_guess
        , c.selected_source_review_status
        , c.selected_source_default_work_arrangement
        , c.selected_source_link_text
        , c.selected_source_link_rel
        , c.selected_source_job_url_discovery_reason
        , c.selection_status
        , c.selection_reason
        , c.known_url_match
        , c.duplicate_url_identity
        , c.latest_selected_at
        , c.latest_selector_version
        , c.latest_selection_stage_run_id
        , c.latest_selection_run_id
        , c.latest_selection_source_record_index
        , c.latest_selection_artifact_sha256
        , COALESCE(e.source_domain, c.selected_source_domain) AS source_domain
        , e.latest_page_status
        , e.latest_retrieved_at
        , e.latest_http_status
        , e.latest_final_url
        , e.latest_redirect_chain_json
        , e.latest_content_type
        , e.latest_attempt_count
        , e.latest_extractor
        , e.latest_primary_extractor
        , e.latest_primary_result_json
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
        , e.latest_normalized_text_char_count
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
        , cl.latest_classification_source_candidate_id
        , cl.latest_classification_source_url
        , cl.latest_classification_source_url_identity
        , cl.latest_classification_source_type_guess
        , cl.latest_classification_source_platform_guess
        , cl.latest_classification_source_review_status
        , cl.latest_classification_source_default_work_arrangement
        , cl.latest_classification_source_content_sha256
        , cl.latest_classification_normalized_text_sha256
        , cl.latest_classification_jsonld_sha256
        , cl.latest_classification_declared_language_raw
        , cl.latest_classification_declared_language_tag
        , cl.latest_classification_declared_language_source
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
        , c.latest_selection_run_id IS NOT NULL AS has_selection
        , e.candidate_id IS NOT NULL AS has_extraction
        , cl.candidate_id IS NOT NULL AS has_classification
        , lr.candidate_id IS NOT NULL AS has_lifecycle_recheck
        , ce.candidate_id IS NOT NULL AS has_country_eligibility_evidence
        , (
            SELECT MAX(observed_at)
            FROM UNNEST([
                c.publication_at
                , c.latest_selected_at
                , e.latest_retrieved_at
                , cl.latest_classified_at
                , lr.latest_lifecycle_checked_at
            ]) AS observed_at
        ) AS latest_observed_at
    FROM candidate_base AS c
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

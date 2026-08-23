{{ config(
    materialized='incremental',
    incremental_strategy='delete_insert',
    unique_key='candidate_id',
    on_schema_change='append_new_columns',
    order_by="(ifNull(candidate_id, ''))",
    query_settings={
        'max_threads': 1,
        'max_bytes_before_external_sort': 268435456,
        'max_bytes_before_external_group_by': 268435456,
        'join_algorithm': 'grace_hash',
        'grace_hash_join_initial_buckets': 32,
        'max_memory_usage': 8589934592
    }
) }}

{% set incremental_watermark_ready = is_incremental()
    and relation_has_columns(this, ['dbt_updated_at']) %}
{% set country_incremental_ready = incremental_watermark_ready
    and relation_has_columns(ref('int_wremotely__candidate_country_eligibility'), ['source_landing_run_id']) %}
{% set titles_incremental_ready = incremental_watermark_ready
    and relation_has_columns(ref('int_wremotely__candidate_job_titles'), ['source_updated_at']) %}

with changed_candidates as (
    select candidate_id
    from {{ ref('int_wremotely__latest_selected_job_urls') }}
    {% if incremental_watermark_ready %}
    where dbt_updated_at > (
        select coalesce(max(dbt_updated_at), toDateTime64('1970-01-01 00:00:00', 3))
        from {{ this }}
    )
    {% endif %}

    union all

    select candidate_id
    from {{ ref('int_wremotely__latest_job_facts') }}
    {% if incremental_watermark_ready %}
    where dbt_updated_at > (
        select coalesce(max(dbt_updated_at), toDateTime64('1970-01-01 00:00:00', 3))
        from {{ this }}
    )
    {% endif %}

    union all

    select candidate_id
    from {{ ref('int_wremotely__latest_extraction_page_results') }}
    {% if incremental_watermark_ready %}
    where dbt_updated_at > (
        select coalesce(max(dbt_updated_at), toDateTime64('1970-01-01 00:00:00', 3))
        from {{ this }}
    )
    {% endif %}

    union all

    select candidate_id
    from {{ ref('int_wremotely__latest_classifications') }}
    {% if incremental_watermark_ready %}
    where dbt_updated_at > (
        select coalesce(max(dbt_updated_at), toDateTime64('1970-01-01 00:00:00', 3))
        from {{ this }}
    )
    {% endif %}

    union all

    select candidate_id
    from {{ ref('int_wremotely__latest_lifecycle_rechecks') }}
    {% if incremental_watermark_ready %}
    where dbt_updated_at > (
        select coalesce(max(dbt_updated_at), toDateTime64('1970-01-01 00:00:00', 3))
        from {{ this }}
    )
    {% endif %}

    union all

    select candidate_id
    from {{ ref('int_wremotely__candidate_country_eligibility') }}
    {% if country_incremental_ready %}
    where dbt_updated_at > (
        select coalesce(max(dbt_updated_at), toDateTime64('1970-01-01 00:00:00', 3))
        from {{ this }}
    )
    {% endif %}

    union all

    select candidate_id
    from {{ ref('int_wremotely__candidate_job_titles') }}
    {% if titles_incremental_ready %}
    where source_updated_at > (
        select coalesce(max(dbt_updated_at), toDateTime64('1970-01-01 00:00:00', 3))
        from {{ this }}
    )
    {% endif %}
),

candidate_keys as (
    select distinct candidate_id
    from changed_candidates
),

selected_job_urls as (
    select * except (raw_payload)
    from {{ ref('int_wremotely__latest_selected_job_urls') }} as selected_job_urls
    where selected_job_urls.candidate_id in (
        select candidate_id
        from candidate_keys
    )
),

job_facts as (
    select * except (raw_payload)
    from {{ ref('int_wremotely__latest_job_facts') }} as job_facts
    where job_facts.candidate_id in (
        select candidate_id
        from candidate_keys
    )
),

extractions as (
    select * except (raw_payload)
    from {{ ref('int_wremotely__latest_extraction_page_results') }} as extractions
    where extractions.candidate_id in (
        select candidate_id
        from candidate_keys
    )
),

classifications as (
    select * except (raw_payload)
    from {{ ref('int_wremotely__latest_classifications') }} as classifications
    where classifications.candidate_id in (
        select candidate_id
        from candidate_keys
    )
),

lifecycle_rechecks as (
    select * except (raw_payload)
    from {{ ref('int_wremotely__latest_lifecycle_rechecks') }} as lifecycle_rechecks
    where lifecycle_rechecks.candidate_id in (
        select candidate_id
        from candidate_keys
    )
),

country_eligibility as (
    select * except (raw_payload)
    from {{ ref('int_wremotely__candidate_country_eligibility') }} as country_eligibility
    where country_eligibility.candidate_id in (
        select candidate_id
        from candidate_keys
    )
),

candidate_titles as (
    select candidate_titles.*
    from {{ ref('int_wremotely__candidate_job_titles') }} as candidate_titles
    where candidate_titles.candidate_id in (
        select candidate_id
        from candidate_keys
    )
),

joined as (
    select
        keys.candidate_id as candidate_id
        , coalesce(facts.url, selected.url) as url
        , titles.title
        , titles.title_source
        , titles.source_updated_at as candidate_title_source_updated_at
        , nullIf(trim(facts.latest_job_fact_raw_company_name), '') as company_name
        , nullIf(trim(facts.latest_job_fact_raw_job_location_text), '')
            as candidate_required_location
        , facts.latest_job_fact_raw_date_posted_at as publication_at
        , selected.source_domain as attribution_name
        , selected.source_url as attribution_url
        , selected.source_link_text as snippet
        , selected.normalized_url as selected_normalized_url
        , selected.source_job_url_id as selected_source_job_url_id
        , selected.source_candidate_id as selected_source_candidate_id
        , selected.source_url as selected_source_url
        , selected.source_domain as selected_source_domain
        , selected.source_crawl_run_id as selected_source_crawl_run_id
        , selected.source_url_identity as selected_source_url_identity
        , selected.source_type_guess as selected_source_type_guess
        , selected.source_platform_guess as selected_source_platform_guess
        , selected.source_review_status as selected_source_review_status
        , selected.source_default_work_arrangement as selected_source_default_work_arrangement
        , selected.source_link_text as selected_source_link_text
        , selected.source_link_text_char_count as selected_source_link_text_char_count
        , selected.source_link_title_candidate as selected_source_link_title_candidate
        , selected.source_link_title_candidate_status
            as selected_source_link_title_candidate_status
        , selected.source_link_rel as selected_source_link_rel
        , selected.source_job_url_discovery_reason
            as selected_source_job_url_discovery_reason
        , selected.selection_status
        , selected.selection_reason
        , selected.known_url_match
        , selected.duplicate_url_identity
        , selected.latest_selected_at
        , selected.latest_selector_version
        , selected.latest_selection_stage_run_id
        , selected.latest_selection_run_id
        , selected.latest_selection_source_record_index
        , selected.latest_selection_artifact_sha256
        , facts.latest_job_fact_final_url
        , facts.latest_job_fact_job_identity_url
        , facts.latest_job_fact_final_url_identity_status
        , facts.latest_job_fact_source_domain
        , facts.latest_job_fact_source_candidate_id
        , facts.latest_job_fact_source_url
        , facts.latest_job_fact_source_url_identity
        , facts.latest_job_fact_source_type_guess
        , facts.latest_job_fact_source_platform_guess
        , facts.latest_job_fact_source_review_status
        , facts.latest_job_fact_status
        , facts.latest_job_fact_page_status
        , facts.latest_job_fact_retrieved_at
        , facts.latest_job_fact_extracted_at
        , facts.latest_job_fact_extractor_version
        , facts.latest_job_fact_http_status
        , facts.latest_job_fact_content_type
        , facts.latest_job_fact_source_content_sha256
        , facts.latest_job_fact_raw_html_path
        , facts.latest_job_fact_normalized_text_path
        , facts.latest_job_fact_normalized_text_sha256
        , facts.latest_job_fact_jsonld_path
        , facts.latest_job_fact_jsonld_sha256
        , facts.latest_job_fact_job_posting_count
        , facts.latest_job_fact_jsonld_document_count
        , facts.latest_job_fact_jsonld_parse_error_count
        , facts.latest_job_fact_declared_language_raw
        , facts.latest_job_fact_declared_language_tag
        , facts.latest_job_fact_declared_language_source
        , facts.latest_job_fact_raw_title_values
        , facts.latest_job_fact_raw_title
        , facts.latest_job_fact_raw_company_name_values
        , facts.latest_job_fact_raw_company_name
        , facts.latest_job_fact_raw_description_values
        , facts.latest_job_fact_raw_description as job_description
        , facts.latest_job_fact_raw_base_salary_values
        , facts.latest_job_fact_raw_base_salary_json
        , facts.latest_job_fact_raw_estimated_salary_values
        , facts.latest_job_fact_raw_estimated_salary_json
        , facts.latest_job_fact_raw_employment_type_values
        , facts.latest_job_fact_raw_employment_type
        , facts.latest_job_fact_raw_date_posted_values
        , facts.latest_job_fact_raw_date_posted_at
        , facts.latest_job_fact_raw_valid_through_values
        , facts.latest_job_fact_raw_valid_through_at
        , facts.latest_job_fact_raw_job_location_type_values
        , facts.latest_job_fact_raw_job_location_type
        , facts.latest_job_fact_raw_job_location_values
        , facts.latest_job_fact_raw_job_location_text
        , facts.latest_job_fact_raw_applicant_location_requirement_values
        , facts.latest_job_fact_raw_applicant_location_requirement_text
        , facts.latest_job_fact_raw_work_arrangement
        , facts.latest_job_fact_raw_work_arrangement_evidence
        , facts.latest_job_fact_source_default_work_arrangement
        , facts.latest_job_fact_source_default_country_eligibility_scope
        , facts.latest_job_fact_source_default_country_eligibility_values
        , facts.latest_job_fact_source_default_country_eligibility_evidence
        , facts.latest_job_fact_record_updated_at
        , facts.latest_job_fact_record_updated_by_step
        , facts.latest_job_fact_stage_run_id
        , facts.latest_job_facts_run_id
        , facts.latest_job_fact_source_record_index
        , facts.latest_job_fact_artifact_sha256
        , coalesce(extractions.source_domain, facts.latest_job_fact_source_domain,
            selected.source_domain) as source_domain
        , extractions.latest_page_status
        , extractions.latest_retrieved_at
        , extractions.latest_http_status
        , coalesce(extractions.latest_final_url, facts.latest_job_fact_final_url)
            as latest_final_url
        , coalesce(
            extractions.latest_job_identity_url
            , facts.latest_job_fact_job_identity_url
            , coalesce(facts.url, selected.url)
        ) as job_identity_url
        , coalesce(
            extractions.latest_final_url_identity_status
            , facts.latest_job_fact_final_url_identity_status
            , 'SELECTED_JOB_URL_FALLBACK'
        ) as final_url_identity_status
        , extractions.latest_redirect_chain_json
        , extractions.latest_content_type
        , extractions.latest_attempt_count
        , extractions.latest_extractor
        , extractions.latest_primary_extractor
        , extractions.latest_primary_result_json
        , extractions.latest_robots_txt_allowed
        , extractions.latest_robots_txt_status
        , extractions.latest_robots_txt_http_status
        , extractions.latest_robots_txt_url
        , extractions.latest_robots_txt_error
        , extractions.latest_robots_txt_json
        , extractions.latest_error_type
        , extractions.latest_error
        , extractions.latest_content_sha256
        , extractions.latest_raw_html_path
        , extractions.latest_normalized_text_path
        , extractions.latest_normalized_text_sha256
        , extractions.latest_normalized_text_char_count
        , extractions.latest_jsonld_path
        , extractions.latest_jsonld_sha256
        , extractions.latest_jsonld_document_count
        , extractions.latest_jsonld_parse_error_count
        , extractions.latest_extraction_stage_run_id
        , extractions.latest_extraction_run_id
        , extractions.latest_extraction_source_record_index
        , extractions.latest_extraction_artifact_sha256
        , classifications.latest_classified_at
        , classifications.latest_classifier_version
        , classifications.latest_classifier_model
        , classifications.latest_classification_status
        , classifications.latest_job_posting_type
        , classifications.latest_job_status
        , classifications.latest_remote_scope
        , classifications.latest_country_eligibility_scope
        , classifications.latest_target_country
        , classifications.latest_target_country_code
        , classifications.latest_target_country_eligibility
        , classifications.latest_serving_decision
        , classifications.latest_classification_source_candidate_id
        , classifications.latest_classification_source_url
        , classifications.latest_classification_source_url_identity
        , classifications.latest_classification_source_type_guess
        , classifications.latest_classification_source_platform_guess
        , classifications.latest_classification_source_review_status
        , classifications.latest_classification_source_default_work_arrangement
        , classifications.latest_classification_source_content_sha256
        , classifications.latest_classification_normalized_text_sha256
        , classifications.latest_classification_jsonld_sha256
        , classifications.latest_classification_declared_language_raw
        , classifications.latest_classification_declared_language_tag
        , classifications.latest_classification_declared_language_source
        , classifications.latest_classification_evidence_json
        , classifications.latest_classification_stage_run_id
        , classifications.latest_classification_run_id
        , classifications.latest_classification_source_record_index
        , classifications.latest_classification_artifact_sha256
        , lifecycle_rechecks.latest_lifecycle_checked_at
        , lifecycle_rechecks.latest_lifecycle_checker_version
        , lifecycle_rechecks.latest_lifecycle_page_status
        , lifecycle_rechecks.latest_lifecycle_status
        , lifecycle_rechecks.previous_lifecycle_status
        , lifecycle_rechecks.latest_lifecycle_signal
        , lifecycle_rechecks.latest_lifecycle_http_status
        , lifecycle_rechecks.latest_lifecycle_final_url
        , lifecycle_rechecks.latest_lifecycle_redirect_chain_json
        , lifecycle_rechecks.latest_lifecycle_content_type
        , lifecycle_rechecks.latest_lifecycle_attempt_count
        , lifecycle_rechecks.latest_lifecycle_extractor
        , lifecycle_rechecks.latest_lifecycle_robots_txt_allowed
        , lifecycle_rechecks.latest_lifecycle_robots_txt_status
        , lifecycle_rechecks.latest_lifecycle_robots_txt_http_status
        , lifecycle_rechecks.latest_lifecycle_robots_txt_url
        , lifecycle_rechecks.latest_lifecycle_robots_txt_error
        , lifecycle_rechecks.latest_lifecycle_error_type
        , lifecycle_rechecks.latest_lifecycle_error
        , lifecycle_rechecks.latest_lifecycle_content_sha256
        , lifecycle_rechecks.latest_lifecycle_raw_html_path
        , lifecycle_rechecks.latest_lifecycle_normalized_text_path
        , lifecycle_rechecks.latest_lifecycle_normalized_text_sha256
        , lifecycle_rechecks.latest_lifecycle_jsonld_path
        , lifecycle_rechecks.latest_lifecycle_jsonld_sha256
        , lifecycle_rechecks.latest_lifecycle_evidence_json
        , lifecycle_rechecks.latest_lifecycle_stage_run_id
        , lifecycle_rechecks.latest_lifecycle_recheck_run_id
        , lifecycle_rechecks.latest_lifecycle_source_record_index
        , lifecycle_rechecks.latest_lifecycle_artifact_sha256
        , country_eligibility.validated_country_eligibility_scope
        , country_eligibility.dbt_updated_at as country_eligibility_updated_at
        , ifNull(country_eligibility.eligible_country_codes, [])
            as eligible_country_codes
        , ifNull(country_eligibility.excluded_country_codes, [])
            as excluded_country_codes
        , ifNull(country_eligibility.included_country_group_codes, [])
            as included_country_group_codes
        , ifNull(country_eligibility.excluded_country_group_codes, [])
            as excluded_country_group_codes
        , country_eligibility.has_global_evidence
        , country_eligibility.has_unknown_evidence
        , country_eligibility.country_eligibility_evidence_count
        , country_eligibility.matched_country_evidence_count
        , country_eligibility.matched_country_group_evidence_count
        , selected.latest_selection_run_id is not null as has_selection
        , extractions.candidate_id is not null as has_extraction
        , facts.candidate_id is not null as has_job_facts
        , classifications.candidate_id is not null as has_classification
        , lifecycle_rechecks.candidate_id is not null as has_lifecycle_recheck
        , country_eligibility.candidate_id is not null
            as has_country_eligibility_evidence
    from candidate_keys as keys
    any left join selected_job_urls as selected
        on keys.candidate_id = selected.candidate_id
    any left join job_facts as facts
        on keys.candidate_id = facts.candidate_id
    any left join extractions
        on keys.candidate_id = extractions.candidate_id
    any left join classifications
        on keys.candidate_id = classifications.candidate_id
    any left join lifecycle_rechecks
        on keys.candidate_id = lifecycle_rechecks.candidate_id
    any left join country_eligibility
        on keys.candidate_id = country_eligibility.candidate_id
    any left join candidate_titles as titles
        on keys.candidate_id = titles.candidate_id
),

observed as (
    select
        joined.*
        , nullIf(greatest(
            ifNull(joined.publication_at, toDateTime64('1970-01-01 00:00:00', 3))
            , ifNull(joined.latest_selected_at, toDateTime64('1970-01-01 00:00:00', 3))
            , ifNull(joined.latest_job_fact_retrieved_at, toDateTime64('1970-01-01 00:00:00', 3))
            , ifNull(joined.latest_job_fact_extracted_at, toDateTime64('1970-01-01 00:00:00', 3))
            , ifNull(joined.latest_job_fact_record_updated_at, toDateTime64('1970-01-01 00:00:00', 3))
            , ifNull(joined.latest_retrieved_at, toDateTime64('1970-01-01 00:00:00', 3))
            , ifNull(joined.latest_classified_at, toDateTime64('1970-01-01 00:00:00', 3))
            , ifNull(joined.latest_lifecycle_checked_at, toDateTime64('1970-01-01 00:00:00', 3))
            , ifNull(joined.country_eligibility_updated_at, toDateTime64('1970-01-01 00:00:00', 3))
            , ifNull(joined.candidate_title_source_updated_at, toDateTime64('1970-01-01 00:00:00', 3))
        ), toDateTime64('1970-01-01 00:00:00', 3)) as latest_observed_at
    from joined
)

select
    observed.*
    , observed.latest_observed_at as source_updated_at
    -- dbt_updated_at is the warehouse processing watermark, not a business
    -- timestamp. Posting/selection timestamps may be in the past or future;
    -- using them here can prevent later upstream changes from entering an
    -- incremental rebuild.
    , now64(3) as dbt_updated_at
from observed

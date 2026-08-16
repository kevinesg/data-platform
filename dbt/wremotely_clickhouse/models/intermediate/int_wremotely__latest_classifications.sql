{{ config(materialized='table') }}

with ranked as (
    select
        *
        , row_number() over (
            partition by candidate_id
            order by
                if(classified_at is null, 1, 0)
                , classified_at desc
                , stage_run_id desc
                , classification_run_id desc
                , source_record_index desc
        ) as classification_rank
    from {{ ref('stg_wremotely__classification_classifications') }}
    where candidate_id is not null
)

select
    candidate_id
    , url
    , classified_at as latest_classified_at
    , classifier_version as latest_classifier_version
    , model as latest_classifier_model
    , classification_status as latest_classification_status
    , job_posting_type as latest_job_posting_type
    , job_status as latest_job_status
    , remote_scope as latest_remote_scope
    , country_eligibility_scope as latest_country_eligibility_scope
    , target_country as latest_target_country
    , target_country_code as latest_target_country_code
    , target_country_eligibility as latest_target_country_eligibility
    , serving_decision as latest_serving_decision
    , source_candidate_id as latest_classification_source_candidate_id
    , source_url as latest_classification_source_url
    , source_url_identity as latest_classification_source_url_identity
    , source_type_guess as latest_classification_source_type_guess
    , source_platform_guess as latest_classification_source_platform_guess
    , source_review_status as latest_classification_source_review_status
    , source_default_work_arrangement as latest_classification_source_default_work_arrangement
    , source_content_sha256 as latest_classification_source_content_sha256
    , normalized_text_sha256 as latest_classification_normalized_text_sha256
    , jsonld_sha256 as latest_classification_jsonld_sha256
    , declared_language_raw as latest_classification_declared_language_raw
    , declared_language_tag as latest_classification_declared_language_tag
    , declared_language_source as latest_classification_declared_language_source
    , evidence_json as latest_classification_evidence_json
    , stage_run_id as latest_classification_stage_run_id
    , classification_run_id as latest_classification_run_id
    , source_record_index as latest_classification_source_record_index
    , source_artifact_sha256 as latest_classification_artifact_sha256
    , raw_payload
    , classified_at as source_updated_at
    , now64(3) as dbt_updated_at
from ranked
where classification_rank = 1

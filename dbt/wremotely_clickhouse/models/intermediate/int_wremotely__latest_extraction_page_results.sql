{{ config(materialized='table') }}

with ranked as (
    select
        *
        , row_number() over (
            partition by candidate_id
            order by
                if(retrieved_at is null, 1, 0)
                , retrieved_at desc
                , stage_run_id desc
                , extraction_run_id desc
                , source_record_index desc
        ) as page_result_rank
    from {{ ref('stg_wremotely__extraction_page_results') }}
    where candidate_id is not null
)

select
    candidate_id
    , url
    , source_domain
    , page_status as latest_page_status
    , retrieved_at as latest_retrieved_at
    , http_status as latest_http_status
    , final_url as latest_final_url
    , job_identity_url as latest_job_identity_url
    , final_url_identity_status as latest_final_url_identity_status
    , redirect_chain_json as latest_redirect_chain_json
    , content_type as latest_content_type
    , attempt_count as latest_attempt_count
    , extractor as latest_extractor
    , primary_extractor as latest_primary_extractor
    , primary_result_json as latest_primary_result_json
    , robots_txt_allowed as latest_robots_txt_allowed
    , robots_txt_status as latest_robots_txt_status
    , robots_txt_http_status as latest_robots_txt_http_status
    , robots_txt_url as latest_robots_txt_url
    , robots_txt_error as latest_robots_txt_error
    , robots_txt_json as latest_robots_txt_json
    , error_type as latest_error_type
    , error as latest_error
    , content_sha256 as latest_content_sha256
    , raw_html_path as latest_raw_html_path
    , normalized_text_path as latest_normalized_text_path
    , normalized_text_sha256 as latest_normalized_text_sha256
    , normalized_text_char_count as latest_normalized_text_char_count
    , jsonld_path as latest_jsonld_path
    , jsonld_sha256 as latest_jsonld_sha256
    , jsonld_document_count as latest_jsonld_document_count
    , jsonld_parse_error_count as latest_jsonld_parse_error_count
    , stage_run_id as latest_extraction_stage_run_id
    , extraction_run_id as latest_extraction_run_id
    , source_record_index as latest_extraction_source_record_index
    , source_artifact_sha256 as latest_extraction_artifact_sha256
    , raw_payload
    , retrieved_at as source_updated_at
    , now64(3) as dbt_updated_at
from ranked
where page_result_rank = 1

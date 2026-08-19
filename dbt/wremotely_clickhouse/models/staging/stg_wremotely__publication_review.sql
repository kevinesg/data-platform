{{ config(
    materialized='incremental',
    incremental_strategy='delete_insert',
    unique_key='candidate_id',
    on_schema_change='append_new_columns',
    order_by="(ifNull(candidate_id, ''))"
) }}

with source_rows as (
    select *
    from {{ source('wremotely', 'publication_review') }}
    {% if is_incremental() %}
    where review_updated_at > (
        select coalesce(max(review_updated_at), toDateTime64('1970-01-01 00:00:00', 3))
        from {{ this }}
    )
    {% endif %}
),

latest as (
    select
        candidate_id
        , argMax(source.contract_version, source.review_updated_at) as contract_version
        , argMax(source.review_status, source.review_updated_at) as review_status
        , argMax(source.review_reason_code, source.review_updated_at) as review_reason_code
        , argMax(source.review_reason, source.review_updated_at) as review_reason
        , argMax(source.review_evidence_json, source.review_updated_at) as review_evidence_json
        , argMax(source.content_sha256, source.review_updated_at) as content_sha256
        , argMax(source.canonical_url, source.review_updated_at) as canonical_url
        , argMax(source.title, source.review_updated_at) as title
        , argMax(source.company_name, source.review_updated_at) as company_name
        , argMax(source.source_domain, source.review_updated_at) as source_domain
        , argMax(source.source_review_run_id, source.review_updated_at) as source_review_run_id
        , argMax(source.reviewed_at, source.review_updated_at) as reviewed_at
        , argMax(source.reviewer, source.review_updated_at) as reviewer
        , argMax(source.decision_sha256, source.review_updated_at) as decision_sha256
        , max(source.review_updated_at) as review_updated_at
    from source_rows as source
    group by source.candidate_id
)

select *
from latest

{{ config(
    materialized='table',
    order_by='(source_artifact_sha256, source_record_index, ingest_key)'
) }}

select
    ingest_key
    , landing_run_id
    , landing_file
    , row_number
    , contract_version as raw_contract_version
    , stage_run_id
    , source_step
    , source_run_id as job_facts_run_id
    , source_artifact
    , source_artifact_sha256
    , source_record_index
    , nullIf(JSONExtractString(payload, 'candidate_id'), '') as candidate_id
    , nullIf(JSONExtractString(payload, 'url'), '') as url
    , nullIf(JSONExtractString(payload, 'final_url'), '') as final_url
    , nullIf(JSONExtractString(payload, 'source_domain'), '') as source_domain
    , nullIf(upper(JSONExtractString(payload, 'job_fact_status')), '') as job_fact_status
    , nullIf(upper(JSONExtractString(payload, 'page_status')), '') as page_status
    , parseDateTimeBestEffortOrNull(JSONExtractString(payload, 'retrieved_at')) as retrieved_at
    , parseDateTimeBestEffortOrNull(
        JSONExtractString(payload, 'job_fact_extracted_at')
    ) as job_fact_extracted_at
    , payload as raw_payload
from {{ source('wremotely', 'job_facts') }}

{{ config(materialized='table') }}

with ranked as (
    select
        *
        , row_number() over (
            partition by candidate_id
            order by
                if(job_fact_extracted_at is null, 1, 0)
                , job_fact_extracted_at desc
                , stage_run_id desc
                , job_facts_run_id desc
                , source_record_index desc
        ) as job_fact_rank
    from {{ ref('stg_wremotely__job_facts') }}
    where candidate_id is not null
)

select
    * except (job_fact_rank)
    , coalesce(job_fact_extracted_at, retrieved_at) as source_updated_at
    , now64(3) as dbt_updated_at
from ranked
where job_fact_rank = 1

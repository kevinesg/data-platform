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
    * except (page_result_rank)
    , retrieved_at as source_updated_at
    , now64(3) as dbt_updated_at
from ranked
where page_result_rank = 1

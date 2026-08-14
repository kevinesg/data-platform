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
    * except (classification_rank)
    , classified_at as source_updated_at
    , now64(3) as dbt_updated_at
from ranked
where classification_rank = 1

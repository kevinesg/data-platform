select
    job_id
    , source_updated_at
    , dbt_updated_at
    , _updated_at
from {{ ref('wremotely__serving_jobs') }}
where isNull(source_updated_at)
    or isNull(dbt_updated_at)
    or isNull(_updated_at)
    or _updated_at != dbt_updated_at

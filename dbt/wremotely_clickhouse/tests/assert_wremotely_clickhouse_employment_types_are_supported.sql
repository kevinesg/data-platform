select
    serving.job_id
    , employment_type
from {{ ref('wremotely__serving_jobs') }} as serving
array join serving.employment_types as employment_type
where employment_type not in (
    'FULL_TIME'
    , 'PART_TIME'
    , 'CONTRACTOR'
    , 'TEMPORARY'
    , 'INTERN'
    , 'VOLUNTEER'
    , 'PER_DIEM'
)

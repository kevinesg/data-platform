SELECT
    job_id
    , employment_type
FROM {{ ref('wremotely__serving_jobs') }}
CROSS JOIN UNNEST(employment_types) AS employment_type
WHERE employment_type NOT IN (
    'FULL_TIME'
    , 'PART_TIME'
    , 'CONTRACTOR'
    , 'TEMPORARY'
    , 'INTERN'
    , 'VOLUNTEER'
    , 'PER_DIEM'
)

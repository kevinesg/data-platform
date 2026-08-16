select
    job_id
    , title
from {{ ref('int_wremotely__publishable_job_facts') }}
where empty(trim(ifNull(title, '')))
    or lengthUTF8(trim(title)) > 500

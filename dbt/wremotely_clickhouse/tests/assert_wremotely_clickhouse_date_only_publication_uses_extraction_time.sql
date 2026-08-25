select
    candidate_id
from {{ ref('int_wremotely__current_candidate_facts') }}
where latest_job_fact_raw_date_posted_at is not null
  and latest_job_fact_extracted_at is not null
  and (
      latest_job_fact_raw_date_posted_at > latest_job_fact_extracted_at
      or (
          match(ifNull(latest_job_fact_raw_date_posted_value, ''), '^[0-9]{4}-[0-9]{2}-[0-9]{2}$')
          and toDate(latest_job_fact_raw_date_posted_at)
              = toDate(latest_job_fact_extracted_at)
      )
  )
  and publication_at != latest_job_fact_extracted_at
union all
select
    candidate_id
from {{ ref('int_wremotely__current_candidate_facts') }}
where latest_job_fact_raw_date_posted_at is not null
  and latest_job_fact_extracted_at is not null
  and not (
      latest_job_fact_raw_date_posted_at > latest_job_fact_extracted_at
      or (
          match(ifNull(latest_job_fact_raw_date_posted_value, ''), '^[0-9]{4}-[0-9]{2}-[0-9]{2}$')
          and toDate(latest_job_fact_raw_date_posted_at)
              = toDate(latest_job_fact_extracted_at)
      )
  )
  and publication_at != latest_job_fact_raw_date_posted_at

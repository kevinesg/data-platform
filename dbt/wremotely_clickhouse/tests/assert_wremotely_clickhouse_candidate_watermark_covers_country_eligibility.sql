select
    country_eligibility.candidate_id
from {{ ref('int_wremotely__candidate_country_eligibility') }} as country_eligibility
inner join {{ ref('int_wremotely__current_candidate_facts') }} as candidate_facts
    on country_eligibility.candidate_id = candidate_facts.candidate_id
where country_eligibility.dbt_updated_at > candidate_facts.dbt_updated_at

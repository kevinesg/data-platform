select *
from {{ ref('wremotely__country_aliases') }}
where lowerUTF8(alias) = 'us'

SELECT *
FROM {{ ref('wremotely__country_aliases') }}
WHERE LOWER(alias) = 'us'

select
    lowerUTF8(ifNull(trim(source_platform_guess), '')) as source_platform_guess
    , trim(replaceRegexpAll(lowerUTF8(location_alias), '[^[:alnum:]]+', ' '))
        as normalized_alias
    , uniqExact(country_code) as country_count
    , count() as alias_count
from {{ ref('wremotely__location_country_aliases') }}
group by source_platform_guess, normalized_alias
having normalized_alias = ''
    or country_count != 1
    or alias_count != 1

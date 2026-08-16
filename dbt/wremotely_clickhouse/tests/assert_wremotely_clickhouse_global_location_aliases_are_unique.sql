select
    lowerUTF8(trim(source_platform_guess)) as source_platform_guess
    , trim(replaceRegexpAll(lowerUTF8(location_alias), '[^[:alnum:]]+', ' '))
        as normalized_alias
    , count() as alias_count
from {{ ref('wremotely__global_location_aliases') }}
group by source_platform_guess, normalized_alias
having source_platform_guess = ''
    or normalized_alias = ''
    or alias_count != 1

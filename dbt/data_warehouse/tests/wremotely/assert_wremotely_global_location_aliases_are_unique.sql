WITH normalized_aliases AS (
    SELECT
        LOWER(TRIM(source_platform_guess)) AS source_platform_guess
        , TRIM(
            REGEXP_REPLACE(
                NORMALIZE_AND_CASEFOLD(location_alias, NFKD)
                , r'[^\p{L}\p{N}]+'
                , ' '
            )
        ) AS normalized_alias
        , COUNT(*) AS alias_count
    FROM {{ ref('wremotely__global_location_aliases') }}
    GROUP BY source_platform_guess, normalized_alias
)

SELECT *
FROM normalized_aliases
WHERE source_platform_guess = ''
    OR normalized_alias = ''
    OR alias_count != 1

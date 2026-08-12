WITH normalized_aliases AS (
    SELECT
        LOWER(COALESCE(NULLIF(TRIM(source_platform_guess), ''), ''))
            AS source_platform_guess
        , TRIM(
            REGEXP_REPLACE(
                NORMALIZE_AND_CASEFOLD(location_alias, NFKD)
                , r'[^\p{L}\p{N}]+'
                , ' '
            )
        ) AS normalized_alias
        , COUNT(DISTINCT country_code) AS country_count
        , COUNT(*) AS alias_count
    FROM {{ ref('wremotely__location_country_aliases') }}
    GROUP BY source_platform_guess, normalized_alias
)

SELECT *
FROM normalized_aliases
WHERE normalized_alias = ''
    OR country_count != 1
    OR alias_count != 1

WITH manifest_count AS (
    SELECT COUNT(*) AS row_count
    FROM {{ ref('wremotely__publication_manifest') }}
)

SELECT row_count
FROM manifest_count
WHERE row_count != 1

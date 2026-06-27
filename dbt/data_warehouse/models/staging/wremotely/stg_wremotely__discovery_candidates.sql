WITH raw_candidates AS (
    SELECT *
    FROM {{ source('wremotely', 'discovery_candidates') }}
),

renamed AS (
    SELECT
        stage_run_id
        , source_run_id AS discovery_run_id
        , source_record_index
        , source_artifact_sha256
        , contract_version AS raw_contract_version
        , SAFE_CAST(JSON_VALUE(payload, '$.contract_version') AS INT64) AS payload_contract_version
        , JSON_VALUE(payload, '$.candidate_id') AS candidate_id
        , JSON_VALUE(payload, '$.url') AS url
        , JSON_VALUE(payload, '$.title') AS title
        , JSON_VALUE(payload, '$.company_name') AS company_name
        , JSON_VALUE(payload, '$.candidate_required_location') AS candidate_required_location
        , SAFE_CAST(JSON_VALUE(payload, '$.publication_date') AS TIMESTAMP) AS publication_at
        , JSON_VALUE(payload, '$.attribution_name') AS attribution_name
        , JSON_VALUE(payload, '$.attribution_url') AS attribution_url
        , JSON_VALUE(payload, '$.snippet') AS snippet
        , JSON_QUERY_ARRAY(payload, '$.discoveries') AS discoveries_json
    FROM raw_candidates
)

SELECT *
FROM renamed

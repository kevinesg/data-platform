WITH raw_source_responses AS (
    SELECT *
    FROM {{ source('wremotely', 'discovery_source_responses') }}
),

renamed AS (
    SELECT
        stage_run_id
        , source_run_id AS discovery_run_id
        , source_record_index
        , source_artifact_sha256
        , contract_version AS raw_contract_version
        , SAFE_CAST(JSON_VALUE(payload, '$.contract_version') AS INT64) AS payload_contract_version
        , JSON_VALUE(payload, '$.provider') AS provider
        , JSON_VALUE(payload, '$.query') AS search_query
        , SAFE_CAST(JSON_VALUE(payload, '$.requested_at') AS TIMESTAMP) AS requested_at
        , JSON_QUERY(payload, '$.response') AS response_json
    FROM raw_source_responses
)

SELECT *
FROM renamed

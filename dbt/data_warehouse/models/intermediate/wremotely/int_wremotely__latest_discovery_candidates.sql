WITH source_candidates AS (
    SELECT *
    FROM {{ ref('stg_wremotely__discovery_candidates') }}
),

ranked AS (
    SELECT
        *
        , ROW_NUMBER() OVER (
            PARTITION BY candidate_id
            ORDER BY
                stage_run_id DESC
                , discovery_run_id DESC
                , source_record_index DESC
                , url DESC
        ) AS candidate_observation_rank
    FROM source_candidates
    WHERE candidate_id IS NOT NULL
),

final AS (
    SELECT
        candidate_id
        , url
        , title
        , company_name
        , candidate_required_location
        , publication_at
        , attribution_name
        , attribution_url
        , snippet
        , discoveries_json
        , stage_run_id AS latest_discovery_stage_run_id
        , discovery_run_id AS latest_discovery_run_id
        , source_record_index AS latest_discovery_source_record_index
        , source_artifact_sha256 AS latest_discovery_artifact_sha256
    FROM ranked
    WHERE candidate_observation_rank = 1
)

SELECT *
FROM final

WITH raw_transfers AS (
    SELECT *
    FROM {{ source('personal_finance', 'transfers') }}
),

renamed AS (
    SELECT
        id AS transfer_id
        , DATE(year, month, day) AS transfer_date
        , UPPER(NULLIF(TRIM(movement_type), '')) AS movement_type
        , NULLIF(TRIM(source), '') AS source_account_key
        , NULLIF(TRIM(destination), '') AS destination_account_key
        , SAFE_CAST(source_amount AS NUMERIC) AS source_amount
        , SAFE_CAST(destination_amount AS NUMERIC) AS destination_amount
        , CAST(created_at AS DATETIME) AS source_created_at_pht
        , CAST(updated_at AS DATETIME) AS source_updated_at_pht
        , DATETIME(_extracted_at, 'Asia/Manila') AS extracted_at_pht
        , DATETIME(_inserted_at, 'Asia/Manila') AS inserted_at_pht
        , _is_deleted AS is_deleted
    FROM raw_transfers
)

SELECT *
FROM renamed

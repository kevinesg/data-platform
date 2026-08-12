WITH raw_transactions AS (
    SELECT *
    FROM {{ source('personal_finance', 'transactions') }}
),

renamed AS (
    SELECT
        id AS transaction_id
        , DATE(year, month, day) AS transaction_date
        , COALESCE(SAFE_CAST(NULLIF(TRIM(posted_date), '') AS DATE), DATE(year, month, day)) AS posted_date
        , NULLIF(TRIM(item), '') AS item
        , type AS transaction_type
        , cost AS amount
        , NULLIF(TRIM(`to`), '') AS counterparty
        , NULLIF(TRIM(store), '') AS store
        , NULLIF(TRIM(source), '') AS payment_source
        , NULLIF(TRIM(receipt), '') AS receipt_url
        , NULLIF(TRIM(solo_part), '') AS solo_part
        , CAST(created_at AS DATETIME) AS source_created_at_pht
        , CAST(updated_at AS DATETIME) AS source_updated_at_pht
        , DATETIME(_extracted_at, 'Asia/Manila') AS extracted_at_pht
        , DATETIME(_inserted_at, 'Asia/Manila') AS inserted_at_pht
        , _is_deleted AS is_deleted
    FROM raw_transactions
)

SELECT *
FROM renamed

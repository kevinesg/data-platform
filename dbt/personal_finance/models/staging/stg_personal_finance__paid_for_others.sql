WITH raw_paid_for_others AS (
    SELECT *
    FROM {{ source('personal_finance', 'paid_for_others') }}
),

renamed AS (
    SELECT
        id AS paid_for_others_id
        , SAFE_CAST(posted_date AS DATE) AS posted_date
        , NULLIF(TRIM(item), '') AS item
        , type AS transaction_type
        , cost AS amount
        , NULLIF(TRIM(`to`), '') AS counterparty
        , NULLIF(TRIM(store), '') AS store
        , NULLIF(TRIM(source), '') AS payment_source
        , NULLIF(TRIM(receipt), '') AS receipt_url
        , NULLIF(TRIM(paid_for), '') AS paid_for
        , CAST(created_at AS DATETIME) AS source_created_at_pht
        , CAST(updated_at AS DATETIME) AS source_updated_at_pht
        , DATETIME(_extracted_at, 'Asia/Manila') AS extracted_at_pht
        , DATETIME(_inserted_at, 'Asia/Manila') AS inserted_at_pht
        , _is_deleted AS is_deleted
    FROM raw_paid_for_others
)

SELECT *
FROM renamed

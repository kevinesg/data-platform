WITH source_transaction_types AS (
    SELECT
        'transactions' AS source_table
        , UPPER(NULLIF(TRIM(transaction_type), '')) AS transaction_type
    FROM {{ ref('stg_personal_finance__transactions') }}
    WHERE NOT is_deleted

    UNION DISTINCT

    SELECT
        'pending_transactions' AS source_table
        , UPPER(NULLIF(TRIM(transaction_type), '')) AS transaction_type
    FROM {{ ref('stg_personal_finance__pending_transactions') }}
    WHERE NOT is_deleted
)

SELECT DISTINCT
    s.source_table
    , s.transaction_type
FROM source_transaction_types AS s
LEFT JOIN {{ ref('personal_finance__transaction_type_classification') }} AS c
    ON s.transaction_type = c.transaction_type
WHERE c.transaction_type IS NULL

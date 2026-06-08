WITH active_source_transactions AS (
    SELECT
        transaction_id
        , transaction_date
        , posted_date
        , item
        , UPPER(NULLIF(TRIM(transaction_type), '')) AS transaction_type
        , SAFE_CAST(amount AS NUMERIC) AS amount
        , counterparty
        , store
        , payment_source
        , receipt_url
        , solo_part
        , source_created_at_pht
        , source_updated_at_pht
        , extracted_at_pht
        , inserted_at_pht
    FROM {{ ref('stg_personal_finance__transactions') }}
    WHERE NOT is_deleted
),

classification AS (
    SELECT *
    FROM {{ ref('personal_finance__transaction_type_classification') }}
),

transactions_with_parsed_share AS (
    SELECT
        *
        , CASE
            WHEN ARRAY_LENGTH(SPLIT(solo_part, '/')) = 2 THEN SAFE_DIVIDE(
                SAFE_CAST(TRIM(SPLIT(solo_part, '/')[SAFE_OFFSET(0)]) AS NUMERIC)
                , NULLIF(SAFE_CAST(TRIM(SPLIT(solo_part, '/')[SAFE_OFFSET(1)]) AS NUMERIC), 0)
            )
        END AS parsed_personal_share_fraction
    FROM active_source_transactions
),

transactions_with_share AS (
    SELECT
        *
        , CASE
            WHEN solo_part IS NULL THEN CAST(1 AS NUMERIC)
            WHEN parsed_personal_share_fraction BETWEEN 0 AND 1 THEN parsed_personal_share_fraction
        END AS personal_share_fraction
    FROM transactions_with_parsed_share
),

final AS (
    SELECT
        t.transaction_id
        , t.transaction_date
        , t.posted_date
        , t.item
        , t.transaction_type
        , c.cashflow_type
        , c.is_income_deduction
        , t.amount
        , t.personal_share_fraction
        , ROUND(t.amount * t.personal_share_fraction, 2) AS personal_amount
        , ROUND(t.amount * (1 - t.personal_share_fraction), 2) AS shared_amount
        , t.counterparty
        , t.store
        , COALESCE(t.store, t.counterparty) AS merchant
        , t.payment_source
        , t.receipt_url
        , t.solo_part
        , t.source_created_at_pht
        , t.source_updated_at_pht
        , t.extracted_at_pht
        , t.inserted_at_pht
    FROM transactions_with_share AS t
    LEFT JOIN classification AS c
        ON t.transaction_type = c.transaction_type
)

SELECT *
FROM final

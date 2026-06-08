{{ config(alias="transactions") }}

WITH transactions AS (
    SELECT *
    FROM {{ ref('int_personal_finance__transactions') }}
),

final AS (
    SELECT
        transaction_id
        , transaction_date
        , posted_date
        , DATE_TRUNC(transaction_date, MONTH) AS transaction_month
        , item
        , transaction_type
        , cashflow_type
        , is_income_deduction
        , amount
        , solo_part
        , personal_share_fraction
        , personal_amount
        , shared_amount
        , counterparty
        , merchant
        , payment_source
        , receipt_url
        , source_created_at_pht
        , source_updated_at_pht
        , extracted_at_pht
        , inserted_at_pht
    FROM transactions
)

SELECT *
FROM final

{{ config(alias="monthly_cashflow_by_type") }}

WITH transactions AS (
    SELECT *
    FROM {{ ref('personal_finance__transactions') }}
),

final AS (
    SELECT
        transaction_month
        , cashflow_type
        , transaction_type
        , COUNT(*) AS transaction_count
        , ROUND(SUM(COALESCE(personal_amount, 0)), 2) AS amount
    FROM transactions
    GROUP BY 1, 2, 3
)

SELECT *
FROM final

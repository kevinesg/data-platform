{{ config(alias="monthly_expenses_by_item") }}

WITH transactions AS (
    SELECT *
    FROM {{ ref('personal_finance__transactions') }}
    WHERE cashflow_type = 'EXPENSE'
        AND NOT is_income_deduction
),

final AS (
    SELECT
        transaction_month
        , transaction_type
        , item
        , merchant
        , payment_source
        , COUNT(*) AS transaction_count
        , ROUND(SUM(COALESCE(personal_amount, 0)), 2) AS amount
    FROM transactions
    GROUP BY 1, 2, 3, 4, 5
)

SELECT *
FROM final

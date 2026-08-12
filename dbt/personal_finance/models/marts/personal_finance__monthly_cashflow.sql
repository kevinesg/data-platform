{{ config(alias="monthly_cashflow") }}

WITH transactions AS (
    SELECT *
    FROM {{ ref('int_personal_finance__transactions') }}
),

monthly_cashflow AS (
    SELECT
        DATE_TRUNC(transaction_date, MONTH) AS transaction_month
        , ROUND(
            SUM(
                CASE
                    WHEN cashflow_type = 'EXPENSE' THEN COALESCE(personal_amount, 0)
                    ELSE 0
                END
            )
            , 2
        ) AS gross_expenses
        , ROUND(
            SUM(
                CASE
                    WHEN cashflow_type = 'EXPENSE' AND NOT is_income_deduction THEN COALESCE(personal_amount, 0)
                    ELSE 0
                END
            )
            , 2
        ) AS net_expenses
        , ROUND(
            SUM(
                CASE
                    WHEN cashflow_type = 'INCOME' THEN COALESCE(personal_amount, 0)
                    ELSE 0
                END
            )
            , 2
        ) AS gross_income
        , ROUND(
            SUM(
                CASE
                    WHEN cashflow_type = 'INCOME' THEN COALESCE(personal_amount, 0)
                    WHEN is_income_deduction THEN -COALESCE(personal_amount, 0)
                    ELSE 0
                END
            )
            , 2
        ) AS net_income
    FROM transactions
    GROUP BY 1
),

final AS (
    SELECT
        transaction_month
        , gross_expenses
        , net_expenses
        , gross_income
        , net_income
        , ROUND(net_income - net_expenses, 2) AS savings
        , ROUND(
            SUM(net_income - net_expenses) OVER (
                ORDER BY transaction_month
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            )
            , 2
        ) AS running_savings
    FROM monthly_cashflow
)

SELECT *
FROM final

{{ config(alias="yearly_cashflow") }}

WITH monthly_cashflow AS (
    SELECT *
    FROM {{ ref('personal_finance__monthly_cashflow') }}
),

yearly_cashflow AS (
    SELECT
        EXTRACT(YEAR FROM transaction_month) AS transaction_year
        , ROUND(SUM(gross_expenses), 2) AS gross_expenses
        , ROUND(SUM(net_expenses), 2) AS net_expenses
        , ROUND(SUM(gross_income), 2) AS gross_income
        , ROUND(SUM(net_income), 2) AS net_income
        , ROUND(SUM(savings), 2) AS savings
    FROM monthly_cashflow
    GROUP BY 1
),

final AS (
    SELECT
        transaction_year
        , gross_expenses
        , net_expenses
        , gross_income
        , net_income
        , savings
        , ROUND(
            SUM(savings) OVER (
                ORDER BY transaction_year
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            )
            , 2
        ) AS running_savings
    FROM yearly_cashflow
)

SELECT *
FROM final

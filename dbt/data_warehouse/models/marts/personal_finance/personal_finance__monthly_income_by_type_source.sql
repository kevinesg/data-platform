{{ config(alias="monthly_income_by_type_source") }}

WITH transactions AS (
    SELECT *
    FROM {{ ref('personal_finance__transactions') }}
    WHERE cashflow_type = 'INCOME'
        OR is_income_deduction
),

income_components AS (
    SELECT
        transaction_month
        , CASE
            WHEN cashflow_type = 'INCOME' THEN transaction_type
            WHEN transaction_type = 'SALARY DEDUCTION' THEN 'SALARY'
            WHEN transaction_type = 'TAX' THEN 'INTEREST'
            ELSE transaction_type
        END AS income_type
        , CASE
            WHEN cashflow_type = 'INCOME' THEN payment_source
            WHEN transaction_type = 'SALARY DEDUCTION' THEN counterparty
            WHEN transaction_type = 'TAX' THEN payment_source
            WHEN is_income_deduction THEN counterparty
        END AS income_source
        , CASE
            WHEN cashflow_type = 'INCOME' THEN COALESCE(personal_amount, 0)
            ELSE 0
        END AS gross_income
        , CASE
            WHEN is_income_deduction THEN COALESCE(personal_amount, 0)
            ELSE 0
        END AS income_deductions
        , CASE
            WHEN transaction_type = 'SALARY DEDUCTION' AND COALESCE(item, '') != 'TAX' THEN COALESCE(personal_amount, 0)
            ELSE 0
        END AS salary_deductions
        , CASE
            WHEN transaction_type = 'SALARY DEDUCTION' AND item = 'TAX' THEN COALESCE(personal_amount, 0)
            ELSE 0
        END AS salary_tax
        , CASE
            WHEN transaction_type = 'TAX' THEN COALESCE(personal_amount, 0)
            ELSE 0
        END AS interest_tax
    FROM transactions
),

final AS (
    SELECT
        transaction_month
        , income_type
        , income_source
        , ROUND(SUM(gross_income), 2) AS gross_income
        , ROUND(SUM(income_deductions), 2) AS income_deductions
        , ROUND(SUM(salary_deductions), 2) AS salary_deductions
        , ROUND(SUM(salary_tax), 2) AS salary_tax
        , ROUND(SUM(interest_tax), 2) AS interest_tax
        , ROUND(SUM(salary_tax + interest_tax), 2) AS tax
        , ROUND(SUM(gross_income - income_deductions), 2) AS net_income
    FROM income_components
    GROUP BY 1, 2, 3
)

SELECT *
FROM final

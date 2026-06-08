WITH salary_accounts AS (
    SELECT DISTINCT
        DATE_TRUNC(transaction_date, MONTH) AS transaction_month
        , payment_source AS salary_source
        , counterparty AS salary_account_key
    FROM {{ ref('int_personal_finance__transactions') }}
    WHERE transaction_type = 'SALARY'
        AND payment_source IS NOT NULL
        AND counterparty IS NOT NULL
),

income_deductions AS (
    SELECT
        t.transaction_id
        , t.transaction_date
        , t.posted_date
        , t.transaction_type
        , t.counterparty
        , CASE
            WHEN t.transaction_type = 'SALARY DEDUCTION' THEN s.salary_account_key
            ELSE t.counterparty
        END AS expected_account_reference
    FROM {{ ref('int_personal_finance__transactions') }} AS t
    LEFT JOIN salary_accounts AS s
        ON t.transaction_type = 'SALARY DEDUCTION'
        AND DATE_TRUNC(t.transaction_date, MONTH) = s.transaction_month
        AND t.counterparty = s.salary_source
    WHERE t.is_income_deduction
        AND t.amount IS NOT NULL
        AND t.posted_date >= DATE '2026-06-01'
        AND t.posted_date <= CURRENT_DATE('Asia/Manila')
)

SELECT
    d.transaction_id
    , d.transaction_date
    , d.posted_date
    , d.transaction_type
    , d.counterparty
    , d.expected_account_reference
    , m.account_reference
    , m.movement_direction
FROM income_deductions AS d
LEFT JOIN {{ ref('int_personal_finance__account_balance_movements') }} AS m
    ON d.transaction_id = m.movement_id
    AND m.movement_source = 'TRANSACTION'
WHERE d.expected_account_reference IS NULL
    OR m.movement_id IS NULL
    OR m.account_reference != d.expected_account_reference
    OR m.movement_direction != 'DECREASE'

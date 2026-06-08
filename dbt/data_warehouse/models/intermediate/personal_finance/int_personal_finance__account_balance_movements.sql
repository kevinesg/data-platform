WITH balance_scope AS (
    SELECT DATE '2026-06-01' AS balance_start_date
),

accounts AS (
    SELECT *
    FROM {{ ref('stg_personal_finance__accounts') }}
    WHERE NOT is_deleted
),

first_opening_balance AS (
    SELECT
        destination_account_key AS account_key
        , MIN(transfer_date) AS first_opening_balance_date
    FROM {{ ref('stg_personal_finance__transfers') }}
    WHERE NOT is_deleted
        AND movement_type = 'OPENING BALANCE'
    GROUP BY 1
),

transactions AS (
    SELECT *
    FROM {{ ref('int_personal_finance__transactions') }}
),

salary_accounts AS (
    SELECT DISTINCT
        DATE_TRUNC(transaction_date, MONTH) AS transaction_month
        , payment_source AS salary_source
        , counterparty AS salary_account_key
    FROM transactions
    WHERE transaction_type = 'SALARY'
        AND payment_source IS NOT NULL
        AND counterparty IS NOT NULL
),

transaction_entries AS (
    SELECT
        t.transaction_id AS movement_id
        , 'TRANSACTION' AS movement_source
        , t.posted_date AS balance_date
        , CASE
            WHEN t.cashflow_type = 'INCOME' THEN t.counterparty
            WHEN t.transaction_type = 'SALARY DEDUCTION' THEN s.salary_account_key
            WHEN t.is_income_deduction THEN t.counterparty
            ELSE t.payment_source
        END AS account_reference
        , CASE
            WHEN t.cashflow_type = 'INCOME' THEN 'INCREASE'
            ELSE 'DECREASE'
        END AS movement_direction
        , t.amount AS amount_native
    FROM transactions AS t
    LEFT JOIN salary_accounts AS s
        ON t.transaction_type = 'SALARY DEDUCTION'
        AND DATE_TRUNC(t.transaction_date, MONTH) = s.transaction_month
        AND t.counterparty = s.salary_source
    WHERE t.amount IS NOT NULL
        AND (
            (
                t.cashflow_type = 'INCOME'
                AND t.counterparty IS NOT NULL
            )
            OR (
                t.cashflow_type = 'EXPENSE'
                AND (
                    (
                        t.transaction_type = 'SALARY DEDUCTION'
                        AND s.salary_account_key IS NOT NULL
                    )
                    OR (
                        t.is_income_deduction
                        AND t.transaction_type != 'SALARY DEDUCTION'
                        AND t.counterparty IS NOT NULL
                    )
                    OR (
                        NOT t.is_income_deduction
                        AND t.payment_source IS NOT NULL
                    )
                )
            )
        )
),

paid_for_others_entries AS (
    SELECT
        paid_for_others_id AS movement_id
        , 'PAID_FOR_OTHERS' AS movement_source
        , posted_date AS balance_date
        , payment_source AS account_reference
        , 'DECREASE' AS movement_direction
        , SAFE_CAST(amount AS NUMERIC) AS amount_native
    FROM {{ ref('stg_personal_finance__paid_for_others') }}
    WHERE NOT is_deleted
        AND payment_source IS NOT NULL
        AND amount IS NOT NULL
),

transfer_source_entries AS (
    SELECT
        transfer_id AS movement_id
        , movement_type AS movement_source
        , transfer_date AS balance_date
        , source_account_key AS account_reference
        , 'DECREASE' AS movement_direction
        , source_amount AS amount_native
    FROM {{ ref('stg_personal_finance__transfers') }}
    WHERE NOT is_deleted
        AND movement_type IN ('TRANSFER', 'OUTFLOW')
        AND source_account_key IS NOT NULL
        AND source_amount IS NOT NULL
),

transfer_destination_entries AS (
    SELECT
        transfer_id AS movement_id
        , movement_type AS movement_source
        , transfer_date AS balance_date
        , destination_account_key AS account_reference
        , CASE
            WHEN movement_type = 'OPENING BALANCE' THEN 'OPENING'
            ELSE 'INCREASE'
        END AS movement_direction
        , destination_amount AS amount_native
    FROM {{ ref('stg_personal_finance__transfers') }}
    WHERE NOT is_deleted
        AND movement_type IN ('OPENING BALANCE', 'TRANSFER', 'INFLOW')
        AND destination_account_key IS NOT NULL
        AND destination_amount IS NOT NULL
),

movement_entries AS (
    SELECT * FROM transaction_entries
    UNION ALL
    SELECT * FROM paid_for_others_entries
    UNION ALL
    SELECT * FROM transfer_source_entries
    UNION ALL
    SELECT * FROM transfer_destination_entries
),

resolved_movements AS (
    SELECT
        m.movement_id
        , m.movement_source
        , m.balance_date
        , m.account_reference
        , m.movement_direction
        , a.account_id
        , a.account_key
        , a.account_name
        , a.parent_name
        , a.account_type
        , a.account_subtype
        , a.currency
        , a.account_status
        , m.amount_native
        , CASE
            WHEN m.movement_direction = 'OPENING' THEN m.amount_native
            WHEN a.account_type = 'CREDIT' AND m.movement_direction = 'INCREASE' THEN -m.amount_native
            WHEN a.account_type = 'CREDIT' AND m.movement_direction = 'DECREASE' THEN m.amount_native
            WHEN m.movement_direction = 'INCREASE' THEN m.amount_native
            WHEN m.movement_direction = 'DECREASE' THEN -m.amount_native
        END AS movement_amount_native
        , CASE a.currency
            WHEN 'PHP' THEN CAST(1 AS NUMERIC)
            WHEN 'USD' THEN CAST(61.47 AS NUMERIC)
            WHEN 'JPY' THEN CAST(0.39 AS NUMERIC)
        END AS php_rate
        , ob.first_opening_balance_date
    FROM movement_entries AS m
    LEFT JOIN accounts AS a
        ON m.account_reference = a.account_key
    LEFT JOIN first_opening_balance AS ob
        ON a.account_key = ob.account_key
    CROSS JOIN balance_scope
    WHERE m.balance_date >= balance_scope.balance_start_date
        AND m.balance_date <= CURRENT_DATE('Asia/Manila')
        AND (
            m.balance_date >= COALESCE(ob.first_opening_balance_date, DATE '0001-01-01')
            OR m.movement_direction = 'OPENING'
        )
),

final AS (
    SELECT
        movement_id
        , movement_source
        , balance_date
        , account_reference
        , movement_direction
        , account_id
        , account_key
        , account_name
        , parent_name
        , account_type
        , account_subtype
        , currency
        , account_status
        , movement_amount_native
        , php_rate
        , ROUND(movement_amount_native * php_rate, 2) AS movement_amount_php_estimated
        , first_opening_balance_date
    FROM resolved_movements
)

SELECT *
FROM final

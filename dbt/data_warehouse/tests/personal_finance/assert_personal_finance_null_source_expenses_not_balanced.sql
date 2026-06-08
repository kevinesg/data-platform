SELECT
    t.transaction_id
    , t.transaction_date
    , t.payment_source
FROM {{ ref('int_personal_finance__transactions') }} AS t
INNER JOIN {{ ref('int_personal_finance__account_balance_movements') }} AS m
    ON t.transaction_id = m.movement_id
    AND m.movement_source = 'TRANSACTION'
WHERE t.cashflow_type = 'EXPENSE'
    AND NOT t.is_income_deduction
    AND t.payment_source IS NULL

SELECT *
FROM {{ ref('personal_finance__current_account_balances') }}
WHERE account_status = 'CLOSED'
    AND (
        balance_native != CAST(0 AS NUMERIC)
        OR balance_php_estimated != CAST(0 AS NUMERIC)
        OR net_worth_balance_php_estimated != CAST(0 AS NUMERIC)
    )

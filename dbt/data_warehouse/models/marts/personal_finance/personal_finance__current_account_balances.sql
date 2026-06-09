{{ config(alias="current_account_balances") }}

WITH daily_account_balances AS (
    SELECT *
    FROM {{ ref('personal_finance__daily_account_balances') }}
),

latest_balance_date AS (
    SELECT MAX(balance_date) AS balance_date
    FROM daily_account_balances
),

final AS (
    SELECT
        b.* EXCEPT (balance_native, balance_php_estimated, net_worth_balance_php_estimated)
        , CASE
            WHEN b.account_status = 'CLOSED' THEN CAST(0 AS NUMERIC)
            ELSE b.balance_native
        END AS balance_native
        , CASE
            WHEN b.account_status = 'CLOSED' THEN CAST(0 AS NUMERIC)
            ELSE b.balance_php_estimated
        END AS balance_php_estimated
        , CASE
            WHEN b.account_status = 'CLOSED' THEN CAST(0 AS NUMERIC)
            ELSE b.net_worth_balance_php_estimated
        END AS net_worth_balance_php_estimated
    FROM daily_account_balances AS b
    INNER JOIN latest_balance_date AS l
        ON b.balance_date = l.balance_date
)

SELECT *
FROM final

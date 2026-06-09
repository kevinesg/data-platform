WITH daily_balances AS (
    SELECT
        account_id
        , account_key
        , balance_date
        , movement_amount_native
        , movement_amount_php_estimated
        , balance_native
        , balance_php_estimated
        , COALESCE(
            LAG(balance_native) OVER (
                PARTITION BY account_id
                ORDER BY balance_date
            )
            , CAST(0 AS NUMERIC)
        ) AS previous_balance_native
        , COALESCE(
            LAG(balance_php_estimated) OVER (
                PARTITION BY account_id
                ORDER BY balance_date
            )
            , CAST(0 AS NUMERIC)
        ) AS previous_balance_php_estimated
    FROM {{ ref('personal_finance__daily_account_balances') }}
),

reconciled AS (
    SELECT
        account_id
        , account_key
        , balance_date
        , movement_amount_native
        , movement_amount_php_estimated
        , balance_native
        , balance_php_estimated
        , previous_balance_native
        , previous_balance_php_estimated
        , ROUND(balance_native - previous_balance_native, 2) AS balance_delta_native
        , ROUND(balance_php_estimated - previous_balance_php_estimated, 2) AS balance_delta_php_estimated
    FROM daily_balances
)

SELECT
    account_id
    , account_key
    , balance_date
    , movement_amount_native
    , balance_delta_native
    , movement_amount_php_estimated
    , balance_delta_php_estimated
    , balance_native
    , balance_php_estimated
FROM reconciled
WHERE ABS(balance_delta_native - movement_amount_native) > 0.01
    OR ABS(balance_delta_php_estimated - movement_amount_php_estimated) > 0.01

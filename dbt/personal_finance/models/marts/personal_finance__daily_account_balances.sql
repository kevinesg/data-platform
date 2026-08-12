{{ config(alias="daily_account_balances") }}

WITH accounts AS (
    SELECT *
    FROM {{ ref('stg_personal_finance__accounts') }}
    WHERE NOT is_deleted
),

movements AS (
    SELECT *
    FROM {{ ref('int_personal_finance__account_balance_movements') }}
),

first_balance_date AS (
    SELECT
        account_id
        , MIN(balance_date) AS first_balance_date
    FROM movements
    GROUP BY 1
),

date_bounds AS (
    SELECT
        COALESCE(MIN(balance_date), CURRENT_DATE('Asia/Manila')) AS min_balance_date
        , CURRENT_DATE('Asia/Manila') AS max_balance_date
    FROM movements
),

date_spine AS (
    SELECT balance_date
    FROM date_bounds
    CROSS JOIN UNNEST(GENERATE_DATE_ARRAY(min_balance_date, max_balance_date)) AS balance_date
),

account_dates AS (
    SELECT
        d.balance_date
        , a.account_id
        , a.account_key
        , a.account_name
        , a.parent_name
        , a.account_type
        , a.account_subtype
        , a.currency
        , a.account_status
    FROM date_spine AS d
    CROSS JOIN accounts AS a
    LEFT JOIN first_balance_date AS f
        ON a.account_id = f.account_id
    WHERE d.balance_date >= COALESCE(f.first_balance_date, d.balance_date)
),

daily_movements AS (
    SELECT
        balance_date
        , account_id
        , ROUND(SUM(movement_amount_native), 2) AS movement_amount_native
        , ROUND(SUM(movement_amount_php_estimated), 2) AS movement_amount_php_estimated
    FROM movements
    GROUP BY 1, 2
),

final AS (
    SELECT
        ad.balance_date
        , ad.account_id
        , ad.account_key
        , ad.account_name
        , ad.parent_name
        , ad.account_type
        , ad.account_subtype
        , ad.currency
        , ad.account_status
        , COALESCE(dm.movement_amount_native, 0) AS movement_amount_native
        , COALESCE(dm.movement_amount_php_estimated, 0) AS movement_amount_php_estimated
        , ROUND(
            SUM(COALESCE(dm.movement_amount_native, 0)) OVER (
                PARTITION BY ad.account_id
                ORDER BY ad.balance_date
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            )
            , 2
        ) AS balance_native
        , ROUND(
            SUM(COALESCE(dm.movement_amount_php_estimated, 0)) OVER (
                PARTITION BY ad.account_id
                ORDER BY ad.balance_date
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            )
            , 2
        ) AS balance_php_estimated
        , CASE
            WHEN ad.account_type = 'CREDIT' THEN -ROUND(
                SUM(COALESCE(dm.movement_amount_php_estimated, 0)) OVER (
                    PARTITION BY ad.account_id
                    ORDER BY ad.balance_date
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                )
                , 2
            )
            ELSE ROUND(
                SUM(COALESCE(dm.movement_amount_php_estimated, 0)) OVER (
                    PARTITION BY ad.account_id
                    ORDER BY ad.balance_date
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                )
                , 2
            )
        END AS net_worth_balance_php_estimated
    FROM account_dates AS ad
    LEFT JOIN daily_movements AS dm
        ON ad.balance_date = dm.balance_date
        AND ad.account_id = dm.account_id
)

SELECT *
FROM final

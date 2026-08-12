SELECT
    movement_id
    , movement_source
    , balance_date
FROM {{ ref('int_personal_finance__account_balance_movements') }}
WHERE balance_date < DATE '2026-06-01'
    OR balance_date > CURRENT_DATE('Asia/Manila')

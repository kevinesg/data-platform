SELECT
    movement_id
    , movement_source
    , movement_direction
FROM {{ ref('int_personal_finance__account_balance_movements') }}
WHERE movement_source = 'OUTFLOW'
    AND movement_direction != 'DECREASE'

SELECT
    movement_id
    , movement_source
    , account_reference
FROM {{ ref('int_personal_finance__account_balance_movements') }}
WHERE account_reference IS NOT NULL
    AND account_key IS NULL

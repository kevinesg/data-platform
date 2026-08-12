SELECT
    transfer_id
    , movement_type
    , destination_account_key
    , destination_amount
FROM {{ ref('stg_personal_finance__transfers') }}
WHERE NOT is_deleted
    AND movement_type IN ('OPENING BALANCE', 'TRANSFER', 'INFLOW')
    AND (
        destination_account_key IS NULL
        OR destination_amount IS NULL
    )

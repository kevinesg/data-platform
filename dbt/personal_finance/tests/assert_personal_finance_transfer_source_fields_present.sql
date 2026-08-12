SELECT
    transfer_id
    , movement_type
    , source_account_key
    , source_amount
FROM {{ ref('stg_personal_finance__transfers') }}
WHERE NOT is_deleted
    AND movement_type IN ('TRANSFER', 'OUTFLOW')
    AND (
        source_account_key IS NULL
        OR source_amount IS NULL
    )

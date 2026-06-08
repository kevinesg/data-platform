SELECT
    transaction_id
    , amount
    , personal_amount
    , shared_amount
FROM {{ ref('int_personal_finance__transactions') }}
WHERE amount IS NOT NULL
    AND personal_amount IS NOT NULL
    AND shared_amount IS NOT NULL
    AND ABS(amount - (personal_amount + shared_amount)) > 0.01

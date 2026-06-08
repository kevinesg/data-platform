SELECT
    transaction_id
    , transaction_date
    , transaction_type
    , item
    , counterparty
FROM {{ ref('int_personal_finance__transactions') }}
WHERE is_income_deduction
    AND counterparty IS NULL

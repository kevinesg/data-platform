SELECT
    transaction_id
    , solo_part
    , personal_share_fraction
FROM {{ ref('int_personal_finance__transactions') }}
WHERE personal_share_fraction IS NULL
    OR personal_share_fraction < 0
    OR personal_share_fraction > 1

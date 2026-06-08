SELECT DISTINCT
    UPPER(NULLIF(TRIM(s.transaction_type), '')) AS transaction_type
FROM {{ ref('stg_personal_finance__transactions') }} AS s
LEFT JOIN {{ ref('personal_finance__transaction_type_classification') }} AS c
    ON UPPER(NULLIF(TRIM(s.transaction_type), '')) = c.transaction_type
WHERE NOT s.is_deleted
    AND c.transaction_type IS NULL

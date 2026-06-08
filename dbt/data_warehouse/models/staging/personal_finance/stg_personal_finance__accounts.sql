WITH raw_accounts AS (
    SELECT *
    FROM {{ source('personal_finance', 'accounts') }}
),

renamed AS (
    SELECT
        id AS account_id
        , NULLIF(TRIM(account_key), '') AS account_key
        , NULLIF(TRIM(name), '') AS account_name
        , NULLIF(TRIM(parent_name), '') AS parent_name
        , UPPER(NULLIF(TRIM(type), '')) AS account_type
        , UPPER(NULLIF(TRIM(subtype), '')) AS account_subtype
        , UPPER(NULLIF(TRIM(currency), '')) AS currency
        , UPPER(NULLIF(TRIM(status), '')) AS account_status
        , CAST(created_at AS DATETIME) AS source_created_at_pht
        , CAST(updated_at AS DATETIME) AS source_updated_at_pht
        , DATETIME(_extracted_at, 'Asia/Manila') AS extracted_at_pht
        , DATETIME(_inserted_at, 'Asia/Manila') AS inserted_at_pht
        , _is_deleted AS is_deleted
    FROM raw_accounts
)

SELECT *
FROM renamed

{{ config(alias="monthly_income_by_source") }}

WITH income_by_type_source AS (
    SELECT *
    FROM {{ ref('personal_finance__monthly_income_by_type_source') }}
),

final AS (
    SELECT
        transaction_month
        , income_source
        , ROUND(SUM(net_income), 2) AS amount
    FROM income_by_type_source
    GROUP BY 1, 2
)

SELECT *
FROM final

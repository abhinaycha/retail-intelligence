-- Silver Customers: cleaned + PII masked column
{{
  config(
    materialized         = 'incremental',
    unique_key           = 'customer_id',
    incremental_strategy = 'merge',
    tags                 = ['silver','customers']
  )
}}

WITH source AS (
    SELECT * FROM {{ source('bronze', 'bronze_customers') }}
    {% if is_incremental() %}
        WHERE _ingested_at > (SELECT MAX(_ingested_at) FROM {{ this }})
    {% endif %}
),
cleaned AS (
    SELECT
        customer_id,
        first_name,
        last_name,
        CONCAT(first_name, ' ', last_name)                       AS full_name,
        email,
        REGEXP_REPLACE(email, '(?<=.{2}).(?=.*@)', '*')          AS email_masked,
        CAST(age AS INT)                                          AS age,
        city,
        state,
        segment,
        TO_DATE(join_date, 'yyyy-MM-dd')                         AS join_date,
        CAST(is_active AS BOOLEAN)                               AS is_active,
        _ingested_at,
        CURRENT_TIMESTAMP()                                       AS _updated_at
    FROM source
    WHERE customer_id IS NOT NULL
),
enriched AS (
    SELECT *,
        CASE
            WHEN age BETWEEN 18 AND 25 THEN '18-25'
            WHEN age BETWEEN 26 AND 35 THEN '26-35'
            WHEN age BETWEEN 36 AND 50 THEN '36-50'
            ELSE '51+'
        END AS age_band,
        DATEDIFF(CURRENT_DATE(), join_date) AS days_since_join
    FROM cleaned
)
SELECT * FROM enriched

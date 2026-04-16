{{
  config(
    materialized         = 'incremental',
    unique_key           = 'product_id',
    incremental_strategy = 'merge',
    tags                 = ['silver','products']
  )
}}

WITH source AS (
    SELECT * FROM {{ source('bronze', 'bronze_products') }}
    {% if is_incremental() %}
        WHERE _ingested_at > (SELECT MAX(_ingested_at) FROM {{ this }})
    {% endif %}
)
SELECT
    product_id,
    product_name,
    category,
    subcategory,
    CAST(cost_price   AS DOUBLE) AS cost_price,
    CAST(retail_price AS DOUBLE) AS retail_price,
    ROUND((CAST(retail_price AS DOUBLE) - CAST(cost_price AS DOUBLE))
          / NULLIF(CAST(retail_price AS DOUBLE), 0), 4)  AS margin_pct,
    supplier_id,
    CAST(is_active AS BOOLEAN)   AS is_active,
    _ingested_at,
    CURRENT_TIMESTAMP()          AS _updated_at
FROM source
WHERE product_id IS NOT NULL
  AND CAST(cost_price AS DOUBLE) > 0

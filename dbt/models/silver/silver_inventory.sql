{{
  config(
    materialized         = 'incremental',
    unique_key           = 'inventory_id',
    incremental_strategy = 'merge',
    tags                 = ['silver','inventory']
  )
}}

WITH source AS (
    SELECT * FROM {{ source('bronze', 'bronze_inventory') }}
    {% if is_incremental() %}
        WHERE _ingested_at > (SELECT MAX(_ingested_at) FROM {{ this }})
    {% endif %}
)
SELECT
    inventory_id,
    product_id,
    warehouse_id,
    TO_DATE(snapshot_date, 'yyyy-MM-dd')  AS snapshot_date,
    CAST(quantity_on_hand  AS INT)        AS quantity_on_hand,
    CAST(reorder_level     AS INT)        AS reorder_level,
    CAST(is_below_reorder  AS BOOLEAN)    AS is_below_reorder,
    CASE WHEN CAST(quantity_on_hand AS INT) = 0 THEN TRUE ELSE FALSE END AS is_out_of_stock,
    _ingested_at,
    CURRENT_TIMESTAMP() AS _updated_at
FROM source
WHERE inventory_id IS NOT NULL

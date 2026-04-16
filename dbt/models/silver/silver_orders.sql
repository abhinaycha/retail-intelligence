-- Silver Orders: cleaned, typed, enriched, incremental
{{
  config(
    materialized    = 'incremental',
    unique_key      = 'order_id',
    incremental_strategy = 'merge',
    contract        = {"enforced": true},
    tags            = ['silver','orders']
  )
}}

WITH source AS (
    SELECT * FROM {{ source('bronze', 'bronze_orders') }}
    {% if is_incremental() %}
        WHERE _ingested_at > (SELECT MAX(_ingested_at) FROM {{ this }})
    {% endif %}
),
cleaned AS (
    SELECT
        order_id,
        customer_id,
        product_id,
        CAST(quantity      AS INT)    AS quantity,
        CAST(unit_price    AS DOUBLE) AS unit_price,
        CAST(discount_pct  AS DOUBLE) AS discount_pct,
        CAST(gross_amount  AS DOUBLE) AS gross_amount,
        CAST(net_amount    AS DOUBLE) AS net_amount,
        TO_DATE(order_date, 'yyyy-MM-dd')   AS order_date,
        channel,
        region,
        status,
        CAST(ship_days     AS INT)    AS ship_days,
        _ingested_at,
        CURRENT_TIMESTAMP()           AS _updated_at,
        FALSE                         AS is_deleted
    FROM source
    WHERE order_id IS NOT NULL
      AND quantity  > 0
      AND net_amount > 0
),
enriched AS (
    SELECT
        *,
        YEAR(order_date)                    AS order_year,
        MONTH(order_date)                   AS order_month,
        DATE_FORMAT(order_date,'yyyy-MM')   AS order_month_key,
        DAYOFWEEK(order_date)               AS day_of_week,
        ROUND(net_amount / NULLIF(quantity,0), 2) AS avg_unit_net_price,
        CASE
            WHEN discount_pct = 0        THEN 'No Discount'
            WHEN discount_pct <= 0.10    THEN 'Low (0-10%)'
            WHEN discount_pct <= 0.20    THEN 'Medium (10-20%)'
            ELSE 'High (20%+)'
        END AS discount_tier
    FROM cleaned
)
SELECT * FROM enriched

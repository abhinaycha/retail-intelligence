-- Fact table: Orders (core of the star schema)
{{ config(materialized='table', tags=['gold','fact']) }}

SELECT
    {{ dbt_utils.generate_surrogate_key(['o.order_id']) }}      AS order_key,
    o.order_id,
    o.order_date,
    c.customer_key,
    p.product_key,
    o.customer_id,
    o.product_id,
    o.quantity,
    o.unit_price,
    o.discount_pct,
    o.gross_amount,
    o.net_amount,
    o.avg_unit_net_price,
    o.discount_tier,
    o.channel,
    o.region,
    o.status,
    o.ship_days,
    o.order_year,
    o.order_month,
    o.order_month_key,
    o.day_of_week,
    -- Derived metrics
    ROUND(o.net_amount - (o.quantity * p.cost_price), 2)        AS gross_profit,
    ROUND((o.net_amount - (o.quantity * p.cost_price))
          / NULLIF(o.net_amount, 0), 4)                         AS gross_margin_pct,
    CASE WHEN o.status = 'Returned'   THEN 1 ELSE 0 END         AS is_returned,
    CASE WHEN o.status = 'Cancelled'  THEN 1 ELSE 0 END         AS is_cancelled,
    CASE WHEN o.status = 'Completed'  THEN 1 ELSE 0 END         AS is_completed,
    o._updated_at
FROM {{ ref('silver_orders') }}           o
LEFT JOIN {{ ref('dim_customer') }}       c ON o.customer_id  = c.customer_id
LEFT JOIN {{ ref('dim_product') }}        p ON o.product_id   = p.product_id
WHERE o.is_deleted = FALSE

-- Gold: Daily sales aggregation by region + channel
{{ config(materialized='table', tags=['gold','aggregate']) }}

SELECT
    order_date,
    region,
    channel,
    order_year,
    order_month,
    order_month_key,
    COUNT(DISTINCT order_id)        AS total_orders,
    COUNT(DISTINCT customer_id)     AS unique_customers,
    COUNT(DISTINCT product_id)      AS unique_products,
    SUM(quantity)                   AS total_units_sold,
    ROUND(SUM(gross_amount), 2)     AS total_gross_revenue,
    ROUND(SUM(net_amount), 2)       AS total_revenue,
    ROUND(SUM(gross_profit), 2)     AS total_gross_profit,
    ROUND(AVG(gross_margin_pct)*100,2) AS avg_margin_pct,
    ROUND(AVG(net_amount), 2)       AS avg_order_value,
    SUM(is_returned)                AS returned_orders,
    SUM(is_cancelled)               AS cancelled_orders,
    ROUND(SUM(is_returned)*100.0 / NULLIF(COUNT(*),0), 2) AS return_rate_pct
FROM {{ ref('fact_orders') }}
GROUP BY 1,2,3,4,5,6

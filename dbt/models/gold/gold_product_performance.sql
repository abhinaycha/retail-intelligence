{{ config(materialized='table', tags=['gold','aggregate']) }}

SELECT
    p.product_id,
    p.product_name,
    p.category,
    p.subcategory,
    p.supplier_id,
    COUNT(DISTINCT fo.order_id)         AS total_orders,
    SUM(fo.quantity)                    AS total_units_sold,
    ROUND(SUM(fo.net_amount), 2)        AS total_revenue,
    ROUND(SUM(fo.gross_profit), 2)      AS total_gross_profit,
    ROUND(AVG(fo.gross_margin_pct)*100,2) AS avg_margin_pct,
    ROUND(AVG(fo.net_amount), 2)        AS avg_order_value,
    SUM(fo.is_returned)                 AS total_returns,
    ROUND(SUM(fo.is_returned)*100.0 / NULLIF(COUNT(*),0), 2) AS return_rate_pct,
    RANK() OVER (PARTITION BY p.category ORDER BY SUM(fo.net_amount) DESC) AS revenue_rank_in_category
FROM {{ ref('fact_orders') }} fo
JOIN {{ ref('dim_product') }} p ON fo.product_id = p.product_id
GROUP BY 1,2,3,4,5

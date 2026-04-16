{{ config(materialized='table', tags=['gold','aggregate']) }}

WITH latest_snapshot AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY product_id, warehouse_id ORDER BY snapshot_date DESC) AS rn
    FROM {{ ref('silver_inventory') }}
),
current_stock AS (
    SELECT * FROM latest_snapshot WHERE rn = 1
)
SELECT
    cs.product_id,
    cs.warehouse_id,
    cs.snapshot_date       AS latest_snapshot_date,
    cs.quantity_on_hand,
    cs.reorder_level,
    cs.is_below_reorder,
    cs.is_out_of_stock,
    p.product_name,
    p.category,
    p.supplier_id,
    CASE
        WHEN cs.is_out_of_stock  THEN 'Out of Stock'
        WHEN cs.is_below_reorder THEN 'Low Stock'
        ELSE 'Healthy'
    END AS stock_status,
    ROUND(cs.quantity_on_hand * p.cost_price, 2) AS inventory_value
FROM current_stock cs
JOIN {{ ref('dim_product') }} p ON cs.product_id = p.product_id

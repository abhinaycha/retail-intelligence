-- Custom test: no row should have negative net_amount
SELECT order_id, net_amount
FROM {{ ref('silver_orders') }}
WHERE net_amount < 0

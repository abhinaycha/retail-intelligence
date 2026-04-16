-- Custom test: every customer in fact_orders should appear in RFM table
SELECT fo.customer_id
FROM {{ ref('fact_orders') }} fo
LEFT JOIN {{ ref('gold_customer_rfm') }} rfm ON fo.customer_id = rfm.customer_id
WHERE rfm.customer_id IS NULL
  AND fo.is_completed = 1

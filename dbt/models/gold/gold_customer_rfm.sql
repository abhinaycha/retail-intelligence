-- Customer RFM Segmentation (Recency, Frequency, Monetary)
{{ config(materialized='table', tags=['gold','aggregate','ml_feature']) }}

WITH rfm_raw AS (
    SELECT
        customer_id,
        MAX(order_date)                          AS last_order_date,
        COUNT(DISTINCT order_id)                 AS frequency,
        ROUND(SUM(net_amount), 2)                AS monetary,
        DATEDIFF(CURRENT_DATE(), MAX(order_date)) AS recency_days
    FROM {{ ref('fact_orders') }}
    WHERE is_completed = 1
    GROUP BY customer_id
),
rfm_scored AS (
    SELECT *,
        NTILE(5) OVER (ORDER BY recency_days DESC)  AS r_score,
        NTILE(5) OVER (ORDER BY frequency ASC)      AS f_score,
        NTILE(5) OVER (ORDER BY monetary ASC)       AS m_score
    FROM rfm_raw
),
rfm_segmented AS (
    SELECT *,
        CONCAT(r_score, f_score, m_score)            AS rfm_score,
        (r_score + f_score + m_score)                AS rfm_total,
        CASE
            WHEN r_score >= 4 AND f_score >= 4 AND m_score >= 4 THEN 'Champions'
            WHEN r_score >= 3 AND f_score >= 3                   THEN 'Loyal Customers'
            WHEN r_score >= 4                                     THEN 'Recent Customers'
            WHEN f_score >= 4                                     THEN 'Frequent Buyers'
            WHEN m_score >= 4                                     THEN 'High Spenders'
            WHEN r_score <= 2 AND f_score >= 3                   THEN 'At Risk'
            WHEN r_score <= 2 AND f_score <= 2                   THEN 'Lost Customers'
            ELSE 'Potential Loyalists'
        END AS rfm_segment
    FROM rfm_scored
)
SELECT
    r.*,
    c.full_name,
    c.segment,
    c.city,
    c.state,
    c.age_band
FROM rfm_segmented r
JOIN {{ ref('dim_customer') }} c ON r.customer_id = c.customer_id

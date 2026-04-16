{{ config(materialized='table', tags=['gold','dimension']) }}

SELECT
    {{ dbt_utils.generate_surrogate_key(['product_id']) }} AS product_key,
    product_id,
    product_name,
    category,
    subcategory,
    cost_price,
    retail_price,
    margin_pct,
    supplier_id,
    is_active,
    _updated_at
FROM {{ ref('silver_products') }}

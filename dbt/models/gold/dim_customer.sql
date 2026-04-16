-- Dimension: Customer (Star Schema)
{{ config(materialized='table', tags=['gold','dimension']) }}

SELECT
    {{ dbt_utils.generate_surrogate_key(['customer_id']) }} AS customer_key,
    customer_id,
    full_name,
    email_masked       AS email,
    age,
    age_band,
    city,
    state,
    segment,
    join_date,
    days_since_join,
    is_active,
    _updated_at
FROM {{ ref('silver_customers') }}

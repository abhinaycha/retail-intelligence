-- SCD Type 2: Track customer segment & city changes over time
{% snapshot scd_customers %}
{{
    config(
        target_schema   = 'silver',
        unique_key      = 'customer_id',
        strategy        = 'check',
        check_cols      = ['segment', 'city', 'state', 'is_active'],
    )
}}
SELECT
    customer_id,
    full_name,
    email_masked   AS email,
    age,
    age_band,
    city,
    state,
    segment,
    join_date,
    is_active,
    _updated_at
FROM {{ ref('silver_customers') }}
{% endsnapshot %}

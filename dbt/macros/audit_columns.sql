-- Reusable macro: add standard audit columns to any model
{% macro add_audit_columns() %}
    CURRENT_TIMESTAMP()  AS _dbt_created_at,
    '{{ invocation_id }}' AS _dbt_invocation_id,
    '{{ this.name }}'    AS _dbt_model_name
{% endmacro %}

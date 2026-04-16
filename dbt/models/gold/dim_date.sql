-- Date dimension — generated for full date coverage
{{ config(materialized='table', tags=['gold','dimension']) }}

WITH date_spine AS (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2022-01-01' as date)",
        end_date="cast('2025-12-31' as date)"
    ) }}
)
SELECT
    date_day                                    AS date_key,
    date_day                                    AS full_date,
    YEAR(date_day)                              AS year,
    QUARTER(date_day)                           AS quarter,
    MONTH(date_day)                             AS month,
    DATE_FORMAT(date_day,'MMMM')               AS month_name,
    DAYOFMONTH(date_day)                        AS day_of_month,
    DAYOFWEEK(date_day)                         AS day_of_week,
    DATE_FORMAT(date_day,'EEEE')               AS day_name,
    DATE_FORMAT(date_day,'yyyy-MM')            AS year_month,
    DATE_FORMAT(date_day,"yyyy-'Q'Q")          AS year_quarter,
    CASE WHEN DAYOFWEEK(date_day) IN (1,7) THEN TRUE ELSE FALSE END AS is_weekend,
    CASE WHEN MONTH(date_day) IN (11,12)        THEN TRUE ELSE FALSE END AS is_holiday_season
FROM date_spine

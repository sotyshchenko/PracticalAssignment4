{{ config(
    materialized='view'
) }}

{% set selected_month = "2024-06-01" %}

with monthly_revenue as (
    {{ calculate_monthly_revenue(
        selected_month = selected_month
    ) }}
)
select * from monthly_revenue

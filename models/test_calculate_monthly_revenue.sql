{{ config(
    materialized='table'
) }}

-- Define the selected month
{% set selected_month = "2023-05-01" %}

-- Call the macro to calculate monthly revenue for the selected month
{{ calculate_monthly_revenue(
    book_orders_table = ref('stg_bookorders'),
    books_table = ref('stg_books'),
    order_date_column = 'order_date',
    book_id_column = 'book_id',
    price_column = 'price',
    quantity_column = 'quantity',
    selected_month = selected_month
) }}

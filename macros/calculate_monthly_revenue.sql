{% macro calculate_monthly_revenue(selected_month) %}
    {% set stg_bookorders = ref('stg_bookorders') %}
    {% set stg_books = ref('stg_books') %}

    select
        date_trunc('month', {{ stg_bookorders }}.order_date) as month,
        sum({{ stg_books }}.book_price * {{ stg_bookorders }}.quantity) as monthly_revenue
    from {{ stg_bookorders }}
    join {{ stg_books }}
    on {{ stg_bookorders }}.book_id = {{ stg_books }}.book_id
    where date_trunc('month', {{ stg_bookorders }}.order_date) = date_trunc('month', '{{ selected_month }}'::date)
    group by 1
    order by 1
{% endmacro %}

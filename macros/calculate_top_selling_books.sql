{% macro calculate_top_selling_books(start_date, end_date) %}
    {% set stg_bookorders = ref('stg_bookorders') %}
    {% set stg_books = ref('stg_books') %}

    select
        {{ stg_books }}.book_title,
        sum({{ stg_bookorders }}.quantity) as total_sold
    from {{ stg_bookorders }}
    join {{ stg_books }}
    on {{ stg_bookorders }}.book_id = {{ stg_books }}.book_id
    where {{ stg_bookorders }}.order_date between '{{ start_date }}' and '{{ end_date }}'
    group by {{ stg_books }}.book_title
    order by total_sold desc
{% endmacro %}

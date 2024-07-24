{% set start_date = "2024-01-01" %}
{% set end_date = "2024-06-30" %}

with top_selling_books as (
    {{ calculate_top_selling_books(
        start_date = start_date,
        end_date = end_date
    ) }}
)
select * from top_selling_books

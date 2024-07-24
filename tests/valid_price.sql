{% test check_price(model, column) %}

    select
        {{ column }} as price_column
    from {{ model }}
    where {{ column }} < 5.99

{% endtest %}

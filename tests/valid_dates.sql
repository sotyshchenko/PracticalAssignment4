{% test valid_dates(model, column) %}

    select *
    from {{ model }}
    where {{ column }} < '2024-01-01'::date
       or {{ column }} > '2024-07-24'::date

{% endtest %}

{{ config(
    materialized = 'incremental',
    unique_key = 'order_id'
) }}



select
    order_id,
    book_id,
    customer_id,
    order_date,
    case
        when quantity > 50 then 'Corporate'
        when quantity <= 50 and quantity > 5 then 'Book Club'
        else 'Personal'
    end as order_type

from {{ ref('stg_bookorders') }}
where
    status = 'Completed'
    {% if is_incremental() %}
        and order_id > (select max(order_id) from {{ this }})
    {% endif %}
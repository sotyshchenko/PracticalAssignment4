{{ config(
    materialized = 'incremental',
    unique_key = 'order_id'
) }}

select
    order_id,
    payment_method,
    amount,
    row_number() over (order by amount desc) as sorted_amount
from {{ ref('stg_payments') }}
where
    payment_method = 'bank_transfer'
qualify
    sorted_amount <= 10
{% if is_incremental() %}
    and order_id > (select max(order_id) from {{ this }})
{% endif %}


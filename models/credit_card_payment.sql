{{ config(
    materialized = 'incremental',
    unique_key = 'order_id'
) }}



select
    order_id,
    payment_method,
    amount,
    case
        when amount > 2500 then 'large'
        when amount <= 2500 and amount > 500 then 'medium'
        else 'small'
    end as checked_amount

from {{ ref('stg_payments') }}
where
    payment_method = 'credit_card'
    {% if is_incremental() %}
        and order_id > (select max(order_id) from {{ this }})
    {% endif %}
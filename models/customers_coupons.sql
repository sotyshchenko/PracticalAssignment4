{{ config(
    materialized='incremental',
    unique_key='order_id'

) }}


with customers_coupons as (

    select * from {{ ref('stg_payments') }}
             where payment_method = 'coupon'

),

orders as (

    select * from {{ ref('stg_orders') }}
             where status = 'returned'
             {% if is_incremental() %}
  and order_date > (select max(order_date) from {{ this }})
{% endif %}

),

customers as (

    select * from {{ ref('stg_customers') }}

),

final as (
    select customers.first_name,
           customers.last_name,
           orders.order_id,
           orders.order_date,
           customers_coupons.amount
    from orders
    left join customers
        on orders.customer_id = customers.customer_id
    left join customers_coupons
        on orders.order_id = customers_coupons.order_id
)

select * from final
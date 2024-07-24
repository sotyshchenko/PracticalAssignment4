with payments_filtered as (
    select * from {{ ref('stg_payments') }}
             where payment_method = 'credit_card'
),

customers as (
    select * from {{ ref('stg_customers') }}
),

orders as (
    select * from {{ ref('stg_orders') }}
             where status = 'completed'
),

customer_orders as (
    select
        orders.customer_id,
        count(orders.order_id) as total_orders
    from orders
    left join payments_filtered
        on orders.order_id = payments_filtered.order_id
    group by orders.customer_id
),

final as (
    select
        customers.customer_id as customer_id,
        customers.first_name,
        customers.last_name,
        customer_orders.total_orders
    from customers
    left join customer_orders
        on customers.customer_id = customer_orders.customer_id
)

select * from final

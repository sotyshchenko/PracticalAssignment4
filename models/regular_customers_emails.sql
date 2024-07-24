with regular_customers as (

    select customer_id,
        first_name,
        last_name,
        email
        from {{ ref('stg_customers') }}
             where regular = 'true'

)

select * from regular_customers
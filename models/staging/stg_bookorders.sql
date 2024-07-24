with source as (

    select * from {{ ref('raw_book_orders') }}

),

renamed as (

    select
        id as order_id,
        book_id,
        customer_id,
        handled_by,
        order_date,
        status,
        quantity

    from source

)

select * from renamed
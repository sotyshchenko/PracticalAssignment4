with source as (

    select * from {{ ref('raw_employees') }}

),

renamed as (

    select
        id as employee_id,
        first_name,
        last_name,
        email,
        phone,
        position

    from source

)

select * from renamed
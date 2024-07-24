{{ config(
    materialized='table'
) }}

with source as (

    select * from {{ ref('raw_customers') }}

),

renamed as (

    select
        id as customer_id,
        first_name,
        last_name,
        email,
        phone,
        birth_date,
        regular

    from source

)

select * from renamed

with source as (

    select * from {{ ref('raw_authors') }}

),

renamed as (

    select
        id as author_id,
        first_name as author_first_name,
        last_name as author_last_name,
        biography as author_biography

    from source

)

select * from renamed
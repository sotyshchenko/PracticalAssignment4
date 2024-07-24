with source as (

    select * from {{ ref('raw_authors_books') }}

),

renamed as (

    select
        author_id,
        book_id

    from source

)

select * from renamed
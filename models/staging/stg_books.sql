with source as (

    select * from {{ ref('raw_books') }}

),

renamed as (

    select
        id as book_id,
        title as book_title,
        genre as book_genre,
        price as book_price,
        isbn as book_isbn,
        publication_year as book_publication_year

    from source

)

select * from renamed
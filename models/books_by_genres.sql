select
    book_genre,
    count(*) as book_count
from
    {{ ref('stg_books') }}
group by
    book_genre
order by
    book_count desc

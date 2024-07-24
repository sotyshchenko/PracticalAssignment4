select
    author_first_name,
    author_last_name,
    count(ab.book_id) as books_written
from
    {{ ref('stg_authors') }} a
join
    {{ ref('stg_authors_books') }} ab on a.author_id = ab.author_id
group by
    author_first_name,
    author_last_name
having
    count(ab.book_id) >= 2
order by
    books_written desc
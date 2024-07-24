with date_series as (
    {{ generate_date_series('2023-01-01', '2023-01-10') }}
)
select * from date_series
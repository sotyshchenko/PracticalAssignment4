{{ config(
    materialized='incremental',
    unique_key='order_id'

) }}



    select
        order_id,
        customer_id,
        order_date,
        status
from {{ ref('stg_orders') }}
where status = 'completed'
{% if is_incremental() %}
  and order_date > (select max(order_date) from {{ this }})
{% endif %}
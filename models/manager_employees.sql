{{ config(
    materialized='incremental',
    unique_key='employee_id'

) }}



    select
        employee_id,
        first_name,
        last_name,
        phone
from {{ ref('stg_employees') }}
where position = 'Manager'
{% if is_incremental() %}
  and employee_id > (select max(employee_id) from {{ this }})
{% endif %}
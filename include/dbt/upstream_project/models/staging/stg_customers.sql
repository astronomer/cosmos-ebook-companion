{{
    config(
        materialized='view',
        access='public'
    )
}}

select
    customer_id,
    first_name,
    last_name,
    first_name || ' ' || last_name as full_name,
    email
from {{ ref('raw_customers') }}

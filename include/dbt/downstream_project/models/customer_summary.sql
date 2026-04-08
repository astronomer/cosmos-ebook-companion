{{
    config(
        materialized='table'
    )
}}

select
    customer_id,
    full_name,
    total_orders,
    total_spent,
    case
        when total_spent >= 300 then 'Gold'
        when total_spent >= 100 then 'Silver'
        else 'Bronze'
    end as customer_tier
from {{ ref('customer_orders') }}

{{
    config(
        materialized='table'
    )
}}

select
    c.customer_id,
    c.full_name,
    c.email,
    count(o.order_id) as total_orders,
    sum(o.amount) as total_spent
from {{ ref('upstream_project', 'stg_customers') }} c
left join {{ ref('upstream_project', 'stg_orders') }} o
    on c.customer_id = o.customer_id
where o.status = 'completed'
group by 1, 2, 3

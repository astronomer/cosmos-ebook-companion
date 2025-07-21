{{ config(materialized='view', tags=['bronze', 'staging', 'channels']) }}

with mock_usage as (
    select
        ROW_NUMBER() OVER (ORDER BY NULL) as usage_id,
        ((ROW_NUMBER() OVER (ORDER BY NULL) - 1) % {{ var('num_customers') }}) + 1 as customer_id,
        
        case 
            when ROW_NUMBER() OVER (ORDER BY NULL) % 5 = 1 then 'ONLINE'
            when ROW_NUMBER() OVER (ORDER BY NULL) % 5 = 2 then 'MOBILE'
            when ROW_NUMBER() OVER (ORDER BY NULL) % 5 = 3 then 'ATM'
            when ROW_NUMBER() OVER (ORDER BY NULL) % 5 = 4 then 'BRANCH'
            else 'PHONE'
        end as channel_type,
        
        (1 + (ROW_NUMBER() OVER (ORDER BY NULL) % 50)) as usage_count,
        DATEADD(day, (ROW_NUMBER() OVER (ORDER BY NULL) % 365), '2024-01-01'::date) as usage_date,
        
        current_timestamp as last_updated
        
    from TABLE(GENERATOR(ROWCOUNT => ({{ var('num_customers') }} * 10)::int))
)

select *, current_timestamp as dbt_created_at from mock_usage 
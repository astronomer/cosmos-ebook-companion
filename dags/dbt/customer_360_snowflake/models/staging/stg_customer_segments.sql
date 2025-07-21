{{ config(materialized='view', tags=['bronze', 'staging', 'segments']) }}

with mock_segments as (
    select
        ROW_NUMBER() OVER (ORDER BY NULL) as segment_id,
        ((ROW_NUMBER() OVER (ORDER BY NULL) - 1) % {{ var('num_customers') }}) + 1 as customer_id,
        
        case 
            when ROW_NUMBER() OVER (ORDER BY NULL) % 5 = 1 then 'PREMIUM'
            when ROW_NUMBER() OVER (ORDER BY NULL) % 5 = 2 then 'GOLD'
            when ROW_NUMBER() OVER (ORDER BY NULL) % 5 = 3 then 'SILVER'
            when ROW_NUMBER() OVER (ORDER BY NULL) % 5 = 4 then 'BRONZE'
            else 'STANDARD'
        end as segment_name,
        
        DATEADD(day, -(ROW_NUMBER() OVER (ORDER BY NULL) % 365), '2024-01-01'::date) as segment_date,
        
        case 
            when ROW_NUMBER() OVER (ORDER BY NULL) % 20 = 1 then 'INACTIVE'
            else 'ACTIVE'
        end as segment_status,
        
        current_timestamp as last_updated
        
    from TABLE(GENERATOR(ROWCOUNT => ({{ var('num_customers') }} * 3)::int))
)

select *, current_timestamp as dbt_created_at from mock_segments 
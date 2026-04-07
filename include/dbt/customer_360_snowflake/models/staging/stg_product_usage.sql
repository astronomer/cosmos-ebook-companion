{{ config(materialized='view', tags=['bronze', 'staging', 'products']) }}

with mock_usage as (
    select
        ROW_NUMBER() OVER (ORDER BY NULL) as usage_id,
        ((ROW_NUMBER() OVER (ORDER BY NULL) - 1) % {{ var('num_customers') }}) + 1 as customer_id,
        
        case 
            when ROW_NUMBER() OVER (ORDER BY NULL) % 6 = 1 then 'CHCK001'
            when ROW_NUMBER() OVER (ORDER BY NULL) % 6 = 2 then 'SAVE001'
            when ROW_NUMBER() OVER (ORDER BY NULL) % 6 = 3 then 'CD001'
            when ROW_NUMBER() OVER (ORDER BY NULL) % 6 = 4 then 'INVT001'
            when ROW_NUMBER() OVER (ORDER BY NULL) % 6 = 5 then 'LOAN001'
            else 'CARD001'
        end as product_id,
        
        DATEADD(day, (ROW_NUMBER() OVER (ORDER BY NULL) % 365), '2024-01-01'::date) as usage_date,
        
        (1 + (ROW_NUMBER() OVER (ORDER BY NULL) % 50)) as usage_count,
        
        case 
            when ROW_NUMBER() OVER (ORDER BY NULL) % 3 = 1 then 'ACTIVE'
            when ROW_NUMBER() OVER (ORDER BY NULL) % 3 = 2 then 'MODERATE'
            else 'LOW'
        end as usage_level,
        
        current_timestamp as last_updated
        
    from TABLE(GENERATOR(ROWCOUNT => ({{ var('num_customers') }} * 12)::int))
)

select *, current_timestamp as dbt_created_at from mock_usage 
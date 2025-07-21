{{ config(
    materialized='view',
    tags=['bronze', 'staging', 'insurance', 'coverage']
) }}

with mock_insurance as (
    select
        ROW_NUMBER() OVER (ORDER BY NULL) as policy_id,
        ((ROW_NUMBER() OVER (ORDER BY NULL) - 1) % {{ var('num_customers') }}) + 1 as customer_id,
        
        case 
            when ROW_NUMBER() OVER (ORDER BY NULL) % 4 = 1 then 'LIFE'
            when ROW_NUMBER() OVER (ORDER BY NULL) % 4 = 2 then 'AUTO'
            when ROW_NUMBER() OVER (ORDER BY NULL) % 4 = 3 then 'HOME'
            else 'HEALTH'
        end as policy_type,
        
        DATEADD(day, -(ROW_NUMBER() OVER (ORDER BY NULL) % 1095), '2024-01-01'::date) as policy_start_date,
        
        case 
            when ROW_NUMBER() OVER (ORDER BY NULL) % 10 = 1 then 'LAPSED'
            when ROW_NUMBER() OVER (ORDER BY NULL) % 20 = 1 then 'CANCELLED'
            else 'ACTIVE'
        end as policy_status,
        
        current_timestamp as last_updated
        
    from TABLE(GENERATOR(ROWCOUNT => ({{ var('num_customers') }} * 2)::int))
)

select *, current_timestamp as dbt_created_at from mock_insurance 
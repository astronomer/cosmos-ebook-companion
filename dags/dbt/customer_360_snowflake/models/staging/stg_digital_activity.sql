{{ config(materialized='view', tags=['bronze', 'staging', 'digital']) }}

with mock_digital as (
    select
        ROW_NUMBER() OVER (ORDER BY NULL) as activity_id,
        ((ROW_NUMBER() OVER (ORDER BY NULL) - 1) % {{ var('num_customers') }}) + 1 as customer_id,
        
        case 
            when ROW_NUMBER() OVER (ORDER BY NULL) % 6 = 1 then 'LOGIN'
            when ROW_NUMBER() OVER (ORDER BY NULL) % 6 = 2 then 'BALANCE_CHECK'
            when ROW_NUMBER() OVER (ORDER BY NULL) % 6 = 3 then 'TRANSFER'
            when ROW_NUMBER() OVER (ORDER BY NULL) % 6 = 4 then 'BILL_PAY'
            when ROW_NUMBER() OVER (ORDER BY NULL) % 6 = 5 then 'MOBILE_DEPOSIT'
            else 'ACCOUNT_SETTINGS'
        end as activity_type,
        
        case 
            when ROW_NUMBER() OVER (ORDER BY NULL) % 2 = 1 then 'MOBILE_APP'
            else 'WEB_PORTAL'
        end as platform,
        
        DATEADD(minute, 
            (ROW_NUMBER() OVER (ORDER BY NULL) % 1440), 
            DATEADD(day, (ROW_NUMBER() OVER (ORDER BY NULL) % 365), '2024-01-01'::timestamp)
        ) as activity_timestamp,
        
        current_timestamp as last_updated
        
    from TABLE(GENERATOR(ROWCOUNT => ({{ var('num_customers') }} * 30)::int))
)

select *, current_timestamp as dbt_created_at from mock_digital 
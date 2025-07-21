{{ config(materialized='view', tags=['bronze', 'staging', 'alerts']) }}

with mock_alerts as (
    select
        ROW_NUMBER() OVER (ORDER BY NULL) as alert_id,
        ((ROW_NUMBER() OVER (ORDER BY NULL) - 1) % ({{ var('num_customers') }} * {{ var('num_accounts_multiplier') }})::int) + 1 as account_id,
        
        case 
            when ROW_NUMBER() OVER (ORDER BY NULL) % 5 = 1 then 'LOW_BALANCE'
            when ROW_NUMBER() OVER (ORDER BY NULL) % 5 = 2 then 'LARGE_DEPOSIT'
            when ROW_NUMBER() OVER (ORDER BY NULL) % 5 = 3 then 'UNUSUAL_ACTIVITY'
            when ROW_NUMBER() OVER (ORDER BY NULL) % 5 = 4 then 'PAYMENT_DUE'
            else 'RATE_CHANGE'
        end as alert_type,
        
        DATEADD(day, (ROW_NUMBER() OVER (ORDER BY NULL) % 365), '2024-01-01'::timestamp) as alert_timestamp,
        
        case 
            when ROW_NUMBER() OVER (ORDER BY NULL) % 10 = 1 then 'DISMISSED'
            when ROW_NUMBER() OVER (ORDER BY NULL) % 20 = 1 then 'ACTIONED'
            else 'PENDING'
        end as alert_status,
        
        current_timestamp as last_updated
        
    from TABLE(GENERATOR(ROWCOUNT => ({{ var('num_customers') }} * {{ var('num_accounts_multiplier') }} * 6)::int))
)

select *, current_timestamp as dbt_created_at from mock_alerts 
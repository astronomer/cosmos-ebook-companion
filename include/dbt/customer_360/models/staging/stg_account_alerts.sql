{{ config(materialized='view', tags=['bronze', 'staging', 'alerts']) }}

with mock_alerts as (
    select
        series_value as alert_id,
        ((series_value - 1) % ({{ var('num_customers') }} * {{ var('num_accounts_multiplier') }})::int) + 1 as account_id,
        
        case 
            when series_value % 5 = 1 then 'LOW_BALANCE'
            when series_value % 5 = 2 then 'LARGE_DEPOSIT'
            when series_value % 5 = 3 then 'UNUSUAL_ACTIVITY'
            when series_value % 5 = 4 then 'PAYMENT_DUE'
            else 'RATE_CHANGE'
        end as alert_type,
        
        '2024-01-01'::timestamp + (series_value % 365) * interval '1 day' as alert_timestamp,
        
        case 
            when series_value % 10 = 1 then 'DISMISSED'
            when series_value % 20 = 1 then 'ACTIONED'
            else 'PENDING'
        end as alert_status,
        
        current_timestamp as last_updated
        
    from generate_series(1, ({{ var('num_customers') }} * {{ var('num_accounts_multiplier') }} * 6)::int) as series_value
)

select *, current_timestamp as dbt_created_at from mock_alerts 
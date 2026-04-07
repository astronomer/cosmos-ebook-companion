{{ config(materialized='view', tags=['bronze', 'staging', 'digital']) }}

with mock_digital as (
    select
        series_value as activity_id,
        ((series_value - 1) % {{ var('num_customers') }}) + 1 as customer_id,
        
        case 
            when series_value % 6 = 1 then 'LOGIN'
            when series_value % 6 = 2 then 'BALANCE_CHECK'
            when series_value % 6 = 3 then 'TRANSFER'
            when series_value % 6 = 4 then 'BILL_PAY'
            when series_value % 6 = 5 then 'MOBILE_DEPOSIT'
            else 'ACCOUNT_SETTINGS'
        end as activity_type,
        
        case 
            when series_value % 2 = 1 then 'MOBILE_APP'
            else 'WEB_PORTAL'
        end as platform,
        
        '2024-01-01'::timestamp + (series_value % 365) * interval '1 day' + 
        (series_value % 1440) * interval '1 minute' as activity_timestamp,
        
        current_timestamp as last_updated
        
    from generate_series(1, ({{ var('num_customers') }} * 30)::int) as series_value
)

select *, current_timestamp as dbt_created_at from mock_digital 
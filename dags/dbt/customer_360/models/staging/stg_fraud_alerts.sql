{{ config(materialized='view', tags=['bronze', 'staging', 'fraud']) }}

with mock_fraud as (
    select
        series_value as alert_id,
        ((series_value - 1) % {{ var('num_customers') }}) + 1 as customer_id,
        
        case 
            when series_value % 5 = 1 then 'UNUSUAL_TRANSACTION'
            when series_value % 5 = 2 then 'LOCATION_ANOMALY'
            when series_value % 5 = 3 then 'VELOCITY_CHECK'
            when series_value % 5 = 4 then 'MERCHANT_RISK'
            else 'BEHAVIORAL_PATTERN'
        end as alert_type,
        
        (1 + (series_value % 100)) as risk_score,
        '2024-01-01'::timestamp + (series_value % 365) * interval '1 day' as alert_timestamp,
        
        case 
            when series_value % 20 = 1 then 'CONFIRMED_FRAUD'
            when series_value % 10 = 1 then 'FALSE_POSITIVE'
            else 'UNDER_REVIEW'
        end as alert_status,
        
        current_timestamp as last_updated
        
    from generate_series(1, ({{ var('num_customers') }} * 3)::int) as series_value
)

select *, current_timestamp as dbt_created_at from mock_fraud 
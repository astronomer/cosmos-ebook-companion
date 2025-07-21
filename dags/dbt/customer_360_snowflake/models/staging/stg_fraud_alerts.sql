{{ config(materialized='view', tags=['bronze', 'staging', 'fraud']) }}

with mock_fraud_alerts as (
    select
        ROW_NUMBER() OVER (ORDER BY NULL) as alert_id,
        ((ROW_NUMBER() OVER (ORDER BY NULL) - 1) % {{ var('num_customers') }}) + 1 as customer_id,
        
        case 
            when ROW_NUMBER() OVER (ORDER BY NULL) % 5 = 1 then 'UNUSUAL_SPENDING'
            when ROW_NUMBER() OVER (ORDER BY NULL) % 5 = 2 then 'LOCATION_ANOMALY'
            when ROW_NUMBER() OVER (ORDER BY NULL) % 5 = 3 then 'VELOCITY_CHECK'
            when ROW_NUMBER() OVER (ORDER BY NULL) % 5 = 4 then 'MERCHANT_RISK'
            else 'DEVICE_RISK'
        end as alert_type,
        
        DATEADD(day, (ROW_NUMBER() OVER (ORDER BY NULL) % 365), '2024-01-01'::timestamp) as alert_timestamp,
        
        case 
            when ROW_NUMBER() OVER (ORDER BY NULL) % 3 = 1 then 'HIGH'
            when ROW_NUMBER() OVER (ORDER BY NULL) % 3 = 2 then 'MEDIUM'
            else 'LOW'
        end as risk_level,
        
        case 
            when ROW_NUMBER() OVER (ORDER BY NULL) % 10 = 1 then 'CONFIRMED_FRAUD'
            when ROW_NUMBER() OVER (ORDER BY NULL) % 8 = 1 then 'FALSE_POSITIVE'
            else 'UNDER_REVIEW'
        end as alert_status,
        
        current_timestamp as last_updated
        
    from TABLE(GENERATOR(ROWCOUNT => ({{ var('num_customers') }} * 2)::int))
)

select *, current_timestamp as dbt_created_at from mock_fraud_alerts 
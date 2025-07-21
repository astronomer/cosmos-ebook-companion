{{ config(
    materialized='view',
    tags=['bronze', 'staging', 'risk']
) }}

with mock_risk as (
    select
        ROW_NUMBER() OVER (ORDER BY NULL) as assessment_id,
        ((ROW_NUMBER() OVER (ORDER BY NULL) - 1) % {{ var('num_customers') }}) + 1 as customer_id,
        
        case 
            when ROW_NUMBER() OVER (ORDER BY NULL) % 4 = 1 then 'CREDIT_RISK'
            when ROW_NUMBER() OVER (ORDER BY NULL) % 4 = 2 then 'FRAUD_RISK'
            when ROW_NUMBER() OVER (ORDER BY NULL) % 4 = 3 then 'OPERATIONAL_RISK'
            else 'COMPLIANCE_RISK'
        end as risk_type,
        
        DATEADD(day, -(ROW_NUMBER() OVER (ORDER BY NULL) % 365), '2024-01-01'::date) as assessment_date,
        
        (1 + (ROW_NUMBER() OVER (ORDER BY NULL) % 100)) as risk_score,
        
        case 
            when ROW_NUMBER() OVER (ORDER BY NULL) % 4 = 1 then 'HIGH'
            when ROW_NUMBER() OVER (ORDER BY NULL) % 4 = 2 then 'MEDIUM'
            when ROW_NUMBER() OVER (ORDER BY NULL) % 4 = 3 then 'LOW'
            else 'MINIMAL'
        end as risk_level,
        
        current_timestamp as last_updated
        
    from TABLE(GENERATOR(ROWCOUNT => ({{ var('num_customers') }} * 4)::int))
)

select *, current_timestamp as dbt_created_at from mock_risk 
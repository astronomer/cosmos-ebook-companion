{{ config(materialized='view', tags=['bronze', 'staging', 'retention']) }}

with mock_retention as (
    select
        ROW_NUMBER() OVER (ORDER BY NULL) as event_id,
        ((ROW_NUMBER() OVER (ORDER BY NULL) - 1) % {{ var('num_customers') }}) + 1 as customer_id,
        
        case 
            when ROW_NUMBER() OVER (ORDER BY NULL) % 6 = 1 then 'CHURN_RISK_IDENTIFIED'
            when ROW_NUMBER() OVER (ORDER BY NULL) % 6 = 2 then 'RETENTION_OFFER_SENT'
            when ROW_NUMBER() OVER (ORDER BY NULL) % 6 = 3 then 'CUSTOMER_SAVED'
            when ROW_NUMBER() OVER (ORDER BY NULL) % 6 = 4 then 'ACCOUNT_CLOSED'
            when ROW_NUMBER() OVER (ORDER BY NULL) % 6 = 5 then 'WIN_BACK_CAMPAIGN'
            else 'LOYALTY_PROGRAM_ENROLLED'
        end as event_type,
        
        DATEADD(day, (ROW_NUMBER() OVER (ORDER BY NULL) % 365), '2024-01-01'::date) as event_date,
        
        case 
            when ROW_NUMBER() OVER (ORDER BY NULL) % 4 = 1 then 'SUCCESSFUL'
            when ROW_NUMBER() OVER (ORDER BY NULL) % 4 = 2 then 'FAILED'
            else 'PENDING'
        end as event_outcome,
        
        current_timestamp as last_updated
        
    from TABLE(GENERATOR(ROWCOUNT => ({{ var('num_customers') }} * 2)::int))
)

select *, current_timestamp as dbt_created_at from mock_retention 
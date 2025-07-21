{{ config(materialized='view', tags=['bronze', 'staging', 'retention']) }}

with mock_retention as (
    select
        series_value as event_id,
        ((series_value - 1) % {{ var('num_customers') }}) + 1 as customer_id,
        
        case 
            when series_value % 6 = 1 then 'CHURN_RISK_IDENTIFIED'
            when series_value % 6 = 2 then 'RETENTION_OFFER_SENT'
            when series_value % 6 = 3 then 'CUSTOMER_SAVED'
            when series_value % 6 = 4 then 'ACCOUNT_CLOSED'
            when series_value % 6 = 5 then 'WIN_BACK_CAMPAIGN'
            else 'LOYALTY_PROGRAM_ENROLLED'
        end as event_type,
        
        '2024-01-01'::date + (series_value % 365) * interval '1 day' as event_date,
        
        case 
            when series_value % 4 = 1 then 'SUCCESSFUL'
            when series_value % 4 = 2 then 'FAILED'
            else 'PENDING'
        end as event_outcome,
        
        current_timestamp as last_updated
        
    from generate_series(1, ({{ var('num_customers') }} * 2)::int) as series_value
)

select *, current_timestamp as dbt_created_at from mock_retention 
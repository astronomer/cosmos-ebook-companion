{{ config(materialized='view', tags=['bronze', 'staging', 'kyc']) }}

with mock_kyc as (
    select
        series_value as kyc_id,
        ((series_value - 1) % {{ var('num_customers') }}) + 1 as customer_id,
        
        case 
            when series_value % 3 = 1 then 'INITIAL'
            when series_value % 3 = 2 then 'PERIODIC_REVIEW'
            else 'ENHANCED_DD'
        end as kyc_type,
        
        '2024-01-01'::date - (series_value % 730) * interval '1 day' as review_date,
        
        case 
            when series_value % 20 = 1 then 'FAILED'
            else 'PASSED'
        end as status,
        
        current_timestamp as last_updated
        
    from generate_series(1, ({{ var('num_customers') }} * 3)::int) as series_value
)

select *, current_timestamp as dbt_created_at from mock_kyc 
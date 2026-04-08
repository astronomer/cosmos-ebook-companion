{{ config(materialized='view', tags=['bronze', 'staging', 'compliance']) }}

with mock_compliance as (
    select
        series_value as compliance_id,
        ((series_value - 1) % {{ var('num_customers') }}) + 1 as customer_id,
        
        case 
            when series_value % 4 = 1 then 'AML_SCREENING'
            when series_value % 4 = 2 then 'SANCTIONS_CHECK'
            when series_value % 4 = 3 then 'PEP_SCREENING'
            else 'ADVERSE_MEDIA'
        end as compliance_type,
        
        '2024-01-01'::date - (series_value % 730) * interval '1 day' as check_date,
        
        case 
            when series_value % 50 = 1 then 'FLAGGED'
            else 'CLEAR'
        end as compliance_status,
        
        current_timestamp as last_updated
        
    from generate_series(1, ({{ var('num_customers') }} * 4)::int) as series_value
)

select *, current_timestamp as dbt_created_at from mock_compliance 
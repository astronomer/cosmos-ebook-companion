{{ config(materialized='view', tags=['bronze', 'staging', 'compliance']) }}

with mock_compliance as (
    select
        ROW_NUMBER() OVER (ORDER BY NULL) as compliance_id,
        ((ROW_NUMBER() OVER (ORDER BY NULL) - 1) % {{ var('num_customers') }}) + 1 as customer_id,
        
        case 
            when ROW_NUMBER() OVER (ORDER BY NULL) % 4 = 1 then 'AML_SCREENING'
            when ROW_NUMBER() OVER (ORDER BY NULL) % 4 = 2 then 'SANCTIONS_CHECK'
            when ROW_NUMBER() OVER (ORDER BY NULL) % 4 = 3 then 'PEP_SCREENING'
            else 'ADVERSE_MEDIA'
        end as compliance_type,
        
        DATEADD(day, -(ROW_NUMBER() OVER (ORDER BY NULL) % 730), '2024-01-01'::date) as check_date,
        
        case 
            when ROW_NUMBER() OVER (ORDER BY NULL) % 50 = 1 then 'FLAGGED'
            else 'CLEAR'
        end as compliance_status,
        
        current_timestamp as last_updated
        
    from TABLE(GENERATOR(ROWCOUNT => ({{ var('num_customers') }} * 4)::int))
)

select *, current_timestamp as dbt_created_at from mock_compliance 
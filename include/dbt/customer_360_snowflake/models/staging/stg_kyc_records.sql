{{ config(materialized='view', tags=['bronze', 'staging', 'kyc']) }}

with mock_kyc as (
    select
        ROW_NUMBER() OVER (ORDER BY NULL) as kyc_id,
        ((ROW_NUMBER() OVER (ORDER BY NULL) - 1) % {{ var('num_customers') }}) + 1 as customer_id,
        
        case 
            when ROW_NUMBER() OVER (ORDER BY NULL) % 3 = 1 then 'INITIAL'
            when ROW_NUMBER() OVER (ORDER BY NULL) % 3 = 2 then 'PERIODIC'
            else 'ENHANCED'
        end as kyc_type,
        
        DATEADD(day, -(ROW_NUMBER() OVER (ORDER BY NULL) % 1095), '2024-01-01'::date) as verification_date,
        
        case 
            when ROW_NUMBER() OVER (ORDER BY NULL) % 20 = 1 then 'FAILED'
            when ROW_NUMBER() OVER (ORDER BY NULL) % 10 = 1 then 'PENDING'
            else 'VERIFIED'
        end as verification_status,
        
        current_timestamp as last_updated
        
    from TABLE(GENERATOR(ROWCOUNT => ({{ var('num_customers') }} * 3)::int))
)

select *, current_timestamp as dbt_created_at from mock_kyc 
{{ config(materialized='view', tags=['bronze', 'staging', 'interest']) }}

with mock_interest as (
    select
        ROW_NUMBER() OVER (ORDER BY NULL) as accrual_id,
        ((ROW_NUMBER() OVER (ORDER BY NULL) - 1) % ({{ var('num_customers') }} * {{ var('num_accounts_multiplier') }})::int) + 1 as account_id,
        
        DATEADD(day, (ROW_NUMBER() OVER (ORDER BY NULL) % 365), '2024-01-01'::date) as accrual_date,
        
        (ROW_NUMBER() OVER (ORDER BY NULL) % 50 + 1.00)::numeric(10,2) as accrual_amount,
        
        case 
            when ROW_NUMBER() OVER (ORDER BY NULL) % 3 = 1 then 'EARNED'
            when ROW_NUMBER() OVER (ORDER BY NULL) % 3 = 2 then 'PAID'
            else 'PENDING'
        end as accrual_status,
        
        current_timestamp as last_updated
        
    from TABLE(GENERATOR(ROWCOUNT => ({{ var('num_customers') }} * {{ var('num_accounts_multiplier') }} * 12)::int))
)

select *, current_timestamp as dbt_created_at from mock_interest 
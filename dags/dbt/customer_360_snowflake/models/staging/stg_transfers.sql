{{ config(materialized='view', tags=['bronze', 'staging', 'transfers']) }}

with mock_transfers as (
    select
        ROW_NUMBER() OVER (ORDER BY NULL) as transfer_id,
        ((ROW_NUMBER() OVER (ORDER BY NULL) - 1) % ({{ var('num_customers') }} * {{ var('num_accounts_multiplier') }})::int) + 1 as from_account_id,
        ((ROW_NUMBER() OVER (ORDER BY NULL) - 1) % ({{ var('num_customers') }} * {{ var('num_accounts_multiplier') }})::int) + 1 as to_account_id,
        
        DATEADD(day, (ROW_NUMBER() OVER (ORDER BY NULL) % 365), '2024-01-01'::date) as transfer_date,
        
        (25.00 + (ROW_NUMBER() OVER (ORDER BY NULL) % 4975))::numeric(10,2) as transfer_amount,
        
        case 
            when ROW_NUMBER() OVER (ORDER BY NULL) % 4 = 1 then 'WIRE'
            when ROW_NUMBER() OVER (ORDER BY NULL) % 4 = 2 then 'ACH'
            when ROW_NUMBER() OVER (ORDER BY NULL) % 4 = 3 then 'INTERNAL'
            else 'EXTERNAL'
        end as transfer_type,
        
        case 
            when ROW_NUMBER() OVER (ORDER BY NULL) % 20 = 1 then 'FAILED'
            when ROW_NUMBER() OVER (ORDER BY NULL) % 10 = 1 then 'PENDING'
            else 'COMPLETED'
        end as transfer_status,
        
        current_timestamp as last_updated
        
    from TABLE(GENERATOR(ROWCOUNT => ({{ var('num_customers') }} * {{ var('num_accounts_multiplier') }} * 15)::int))
)

select *, current_timestamp as dbt_created_at from mock_transfers 
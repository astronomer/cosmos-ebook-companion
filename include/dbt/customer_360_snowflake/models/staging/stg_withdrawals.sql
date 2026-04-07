{{ config(
    materialized='view',
    tags=['bronze', 'staging', 'transactions', 'withdrawals']
) }}

with mock_withdrawals as (
    select
        ROW_NUMBER() OVER (ORDER BY NULL) as withdrawal_id,
        ((ROW_NUMBER() OVER (ORDER BY NULL) - 1) % ({{ var('num_customers') }} * {{ var('num_accounts_multiplier') }})::int) + 1 as account_id,
        
        DATEADD(day, (ROW_NUMBER() OVER (ORDER BY NULL) % 365), '2024-01-01'::date) as withdrawal_date,
        
        (20.00 + (ROW_NUMBER() OVER (ORDER BY NULL) % 980))::numeric(10,2) as withdrawal_amount,
        
        case 
            when ROW_NUMBER() OVER (ORDER BY NULL) % 4 = 1 then 'ATM'
            when ROW_NUMBER() OVER (ORDER BY NULL) % 4 = 2 then 'TELLER'
            when ROW_NUMBER() OVER (ORDER BY NULL) % 4 = 3 then 'ONLINE'
            else 'MOBILE'
        end as withdrawal_method,
        
        case 
            when ROW_NUMBER() OVER (ORDER BY NULL) % 25 = 1 then 'FAILED'
            when ROW_NUMBER() OVER (ORDER BY NULL) % 15 = 1 then 'PENDING'
            else 'COMPLETED'
        end as withdrawal_status,
        
        current_timestamp as last_updated
        
    from TABLE(GENERATOR(ROWCOUNT => ({{ var('num_customers') }} * {{ var('num_accounts_multiplier') }} * 25)::int))
)

select *, current_timestamp as dbt_created_at from mock_withdrawals 
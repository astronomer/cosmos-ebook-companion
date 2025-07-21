{{ config(materialized='view', tags=['bronze', 'staging', 'balances']) }}

with mock_balances as (
    select
        ROW_NUMBER() OVER (ORDER BY NULL) as balance_id,
        ((ROW_NUMBER() OVER (ORDER BY NULL) - 1) % ({{ var('num_customers') }} * {{ var('num_accounts_multiplier') }})::int) + 1 as account_id,
        
        (1000 + (ROW_NUMBER() OVER (ORDER BY NULL) % 49000))::numeric(12,2) as balance_amount,
        (500 + (ROW_NUMBER() OVER (ORDER BY NULL) % 2500))::numeric(12,2) as available_balance,
        
        DATEADD(day, (ROW_NUMBER() OVER (ORDER BY NULL) % 365), '2024-01-01'::date) as balance_date,
        
        current_timestamp as last_updated
        
    from TABLE(GENERATOR(ROWCOUNT => ({{ var('num_customers') }} * {{ var('num_accounts_multiplier') }} * 30)::int))
)

select 
    *,
    case 
        when balance_amount >= 50000 then 'HIGH_BALANCE'
        when balance_amount >= 10000 then 'MEDIUM_BALANCE'
        else 'LOW_BALANCE'
    end as balance_category,
    current_timestamp as dbt_created_at
from mock_balances 
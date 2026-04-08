{{ config(materialized='view', tags=['bronze', 'staging', 'balances']) }}

with mock_balances as (
    select
        series_value as balance_id,
        ((series_value - 1) % ({{ var('num_customers') }} * {{ var('num_accounts_multiplier') }})::int) + 1 as account_id,
        
        (1000 + (series_value % 49000))::numeric(12,2) as balance_amount,
        (500 + (series_value % 2500))::numeric(12,2) as available_balance,
        
        '2024-01-01'::date + (series_value % 365) * interval '1 day' as balance_date,
        
        current_timestamp as last_updated
        
    from generate_series(1, ({{ var('num_customers') }} * {{ var('num_accounts_multiplier') }} * 30)::int) as series_value
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
{{ config(
    materialized='view',
    tags=['bronze', 'staging', 'transactions', 'withdrawals']
) }}

with mock_withdrawals as (
    select
        series_value as withdrawal_id,
        'WDL' || lpad(series_value::text, 10, '0') as withdrawal_number,
        ((series_value - 1) % ({{ var('num_customers') }} * {{ var('num_accounts_multiplier') }})::int) + 1 as account_id,
        
        case 
            when series_value % 5 = 1 then 'ATM'
            when series_value % 5 = 2 then 'TELLER'
            when series_value % 5 = 3 then 'ONLINE'
            when series_value % 5 = 4 then 'WIRE_TRANSFER'
            else 'CHECK'
        end as withdrawal_type,
        
        case 
            when series_value % 5 = 1 then (20 + (series_value % 580))::numeric(10,2)
            when series_value % 5 = 2 then (50 + (series_value % 1950))::numeric(10,2)
            when series_value % 5 = 3 then (100 + (series_value % 4900))::numeric(10,2)
            when series_value % 5 = 4 then (1000 + (series_value % 49000))::numeric(10,2)
            else (25 + (series_value % 2975))::numeric(10,2)
        end as withdrawal_amount,
        
        '2024-01-01'::date + (series_value % 365) * interval '1 day' as withdrawal_datetime,
        
        case 
            when series_value % 100 = 1 then 'FAILED'
            when series_value % 50 = 1 then 'PENDING'
            else 'COMPLETED'
        end as withdrawal_status,
        
        current_timestamp as last_updated
        
    from generate_series(1, ({{ var('num_customers') }} * {{ var('num_accounts_multiplier') }} * 15)::int) as series_value
),

enriched_withdrawals as (
    select
        mw.*,
        extract(hour from mw.withdrawal_datetime) as withdrawal_hour,
        
        case 
            when mw.withdrawal_amount >= 5000 then 'LARGE'
            when mw.withdrawal_amount >= 500 then 'MEDIUM'
            else 'SMALL'
        end as amount_category,
        
        current_timestamp as dbt_created_at
        
    from mock_withdrawals mw
)

select * from enriched_withdrawals 
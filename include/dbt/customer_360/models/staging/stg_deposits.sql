{{ config(
    materialized='view',
    tags=['bronze', 'staging', 'transactions', 'deposits']
) }}

with mock_deposits as (
    select
        series_value as deposit_id,
        'DEP' || lpad(series_value::text, 10, '0') as deposit_number,
        ((series_value - 1) % ({{ var('num_customers') }} * {{ var('num_accounts_multiplier') }})::int) + 1 as account_id,
        
        case 
            when series_value % 6 = 1 then 'PAYROLL'
            when series_value % 6 = 2 then 'CASH'
            when series_value % 6 = 3 then 'CHECK'
            when series_value % 6 = 4 then 'WIRE_TRANSFER'
            when series_value % 6 = 5 then 'ACH'
            else 'MOBILE_DEPOSIT'
        end as deposit_type,
        
        case 
            when series_value % 6 = 1 then (2000 + (series_value % 6000))::numeric(10,2)    -- Payroll
            when series_value % 6 = 2 then (50 + (series_value % 950))::numeric(10,2)       -- Cash
            when series_value % 6 = 3 then (100 + (series_value % 2900))::numeric(10,2)     -- Check
            when series_value % 6 = 4 then (5000 + (series_value % 95000))::numeric(10,2)   -- Wire
            when series_value % 6 = 5 then (500 + (series_value % 4500))::numeric(10,2)     -- ACH
            else (25 + (series_value % 975))::numeric(10,2)                                  -- Mobile
        end as deposit_amount,
        
        '2024-01-01'::date + (series_value % 365) * interval '1 day' + 
        (series_value % 1440) * interval '1 minute' as deposit_datetime,
        
        case 
            when series_value % 50 = 1 then 'PENDING'
            when series_value % 100 = 1 then 'HELD'
            else 'CLEARED'
        end as deposit_status,
        
        current_timestamp as last_updated
        
    from generate_series(1, ({{ var('num_customers') }} * {{ var('num_accounts_multiplier') }} * 20)::int) as series_value
),

enriched_deposits as (
    select
        md.*,
        extract(hour from md.deposit_datetime) as deposit_hour,
        extract(dow from md.deposit_datetime) as day_of_week,
        
        case 
            when md.deposit_amount >= 10000 then 'LARGE'
            when md.deposit_amount >= 1000 then 'MEDIUM'
            else 'SMALL'
        end as amount_category,
        
        case 
            when md.deposit_type in ('MOBILE_DEPOSIT', 'ACH') then 'DIGITAL'
            else 'TRADITIONAL'
        end as channel_type,
        
        current_timestamp as dbt_created_at
        
    from mock_deposits md
)

select * from enriched_deposits 
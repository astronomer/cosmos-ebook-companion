{{ config(
    materialized='view',
    tags=['bronze', 'staging', 'transactions', 'deposits']
) }}

with mock_deposits as (
    select
        ROW_NUMBER() OVER (ORDER BY NULL) as deposit_id,
        'DEP' || LPAD(ROW_NUMBER() OVER (ORDER BY NULL)::string, 10, '0') as deposit_number,
        ((ROW_NUMBER() OVER (ORDER BY NULL) - 1) % ({{ var('num_customers') }} * {{ var('num_accounts_multiplier') }})::int) + 1 as account_id,
        
        case 
            when ROW_NUMBER() OVER (ORDER BY NULL) % 6 = 1 then 'PAYROLL'
            when ROW_NUMBER() OVER (ORDER BY NULL) % 6 = 2 then 'CASH'
            when ROW_NUMBER() OVER (ORDER BY NULL) % 6 = 3 then 'CHECK'
            when ROW_NUMBER() OVER (ORDER BY NULL) % 6 = 4 then 'WIRE_TRANSFER'
            when ROW_NUMBER() OVER (ORDER BY NULL) % 6 = 5 then 'ACH'
            else 'MOBILE_DEPOSIT'
        end as deposit_type,
        
        case 
            when ROW_NUMBER() OVER (ORDER BY NULL) % 6 = 1 then (2000 + (ROW_NUMBER() OVER (ORDER BY NULL) % 6000))::numeric(10,2)    -- Payroll
            when ROW_NUMBER() OVER (ORDER BY NULL) % 6 = 2 then (50 + (ROW_NUMBER() OVER (ORDER BY NULL) % 950))::numeric(10,2)       -- Cash
            when ROW_NUMBER() OVER (ORDER BY NULL) % 6 = 3 then (100 + (ROW_NUMBER() OVER (ORDER BY NULL) % 2900))::numeric(10,2)     -- Check
            when ROW_NUMBER() OVER (ORDER BY NULL) % 6 = 4 then (5000 + (ROW_NUMBER() OVER (ORDER BY NULL) % 95000))::numeric(10,2)   -- Wire
            when ROW_NUMBER() OVER (ORDER BY NULL) % 6 = 5 then (500 + (ROW_NUMBER() OVER (ORDER BY NULL) % 4500))::numeric(10,2)     -- ACH
            else (25 + (ROW_NUMBER() OVER (ORDER BY NULL) % 975))::numeric(10,2)                                  -- Mobile
        end as deposit_amount,
        
        DATEADD(minute, 
            (ROW_NUMBER() OVER (ORDER BY NULL) % 1440), 
            DATEADD(day, (ROW_NUMBER() OVER (ORDER BY NULL) % 365), '2024-01-01'::date)
        ) as deposit_datetime,
        
        case 
            when ROW_NUMBER() OVER (ORDER BY NULL) % 50 = 1 then 'PENDING'
            when ROW_NUMBER() OVER (ORDER BY NULL) % 100 = 1 then 'HELD'
            else 'CLEARED'
        end as deposit_status,
        
        current_timestamp as last_updated
        
    from TABLE(GENERATOR(ROWCOUNT => ({{ var('num_customers') }} * {{ var('num_accounts_multiplier') }} * 20)::int))
),

enriched_deposits as (
    select
        md.*,
        EXTRACT(hour from md.deposit_datetime) as deposit_hour,
        EXTRACT(dayofweek FROM md.deposit_datetime) as day_of_week,
        
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
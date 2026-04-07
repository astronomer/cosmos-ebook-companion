{{ config(
    materialized='view',
    tags=['bronze', 'staging', 'fees', 'revenue']
) }}

with mock_fees as (
    select
        ROW_NUMBER() OVER (ORDER BY NULL) as fee_id,
        ((ROW_NUMBER() OVER (ORDER BY NULL) - 1) % ({{ var('num_customers') }} * {{ var('num_accounts_multiplier') }})::int) + 1 as account_id,
        
        case 
            when ROW_NUMBER() OVER (ORDER BY NULL) % 6 = 1 then 'MONTHLY_MAINTENANCE'
            when ROW_NUMBER() OVER (ORDER BY NULL) % 6 = 2 then 'OVERDRAFT'
            when ROW_NUMBER() OVER (ORDER BY NULL) % 6 = 3 then 'ATM_FOREIGN'
            when ROW_NUMBER() OVER (ORDER BY NULL) % 6 = 4 then 'WIRE_TRANSFER'
            when ROW_NUMBER() OVER (ORDER BY NULL) % 6 = 5 then 'STOP_PAYMENT'
            else 'PAPER_STATEMENT'
        end as fee_type,
        
        DATEADD(day, (ROW_NUMBER() OVER (ORDER BY NULL) % 365), '2024-01-01'::date) as fee_date,
        
        case 
            when ROW_NUMBER() OVER (ORDER BY NULL) % 6 = 1 then 15.00
            when ROW_NUMBER() OVER (ORDER BY NULL) % 6 = 2 then 35.00
            when ROW_NUMBER() OVER (ORDER BY NULL) % 6 = 3 then 3.00
            when ROW_NUMBER() OVER (ORDER BY NULL) % 6 = 4 then 25.00
            when ROW_NUMBER() OVER (ORDER BY NULL) % 6 = 5 then 10.00
            else 5.00
        end as fee_amount,
        
        case 
            when ROW_NUMBER() OVER (ORDER BY NULL) % 10 = 1 then 'WAIVED'
            when ROW_NUMBER() OVER (ORDER BY NULL) % 8 = 1 then 'REFUNDED'
            else 'CHARGED'
        end as fee_status,
        
        current_timestamp as last_updated
        
    from TABLE(GENERATOR(ROWCOUNT => ({{ var('num_customers') }} * {{ var('num_accounts_multiplier') }} * 8)::int))
),

enriched_fees as (
    select
        mf.*,
        
        case 
            when mf.fee_amount >= 25.00 then 'HIGH_FEE'
            when mf.fee_amount >= 10.00 then 'MEDIUM_FEE'
            else 'LOW_FEE'
        end as fee_category,
        
        case 
            when mf.fee_type in ('OVERDRAFT', 'STOP_PAYMENT') then 'PENALTY'
            when mf.fee_type = 'MONTHLY_MAINTENANCE' then 'SERVICE'
            else 'TRANSACTION'
        end as fee_classification,
        
        current_timestamp as dbt_created_at
        
    from mock_fees mf
)

select * from enriched_fees 
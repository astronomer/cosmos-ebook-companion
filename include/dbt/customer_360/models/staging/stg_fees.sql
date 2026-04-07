{{ config(
    materialized='view',
    tags=['bronze', 'staging', 'fees', 'revenue']
) }}

with mock_fees as (
    select
        series_value as fee_id,
        'FEE' || lpad(series_value::text, 8, '0') as fee_number,
        ((series_value - 1) % ({{ var('num_customers') }} * {{ var('num_accounts_multiplier') }})::int) + 1 as account_id,
        
        case 
            when series_value % 6 = 1 then 'MONTHLY_MAINTENANCE'
            when series_value % 6 = 2 then 'OVERDRAFT'
            when series_value % 6 = 3 then 'ATM_FEE'
            when series_value % 6 = 4 then 'WIRE_TRANSFER'
            when series_value % 6 = 5 then 'FOREIGN_TRANSACTION'
            else 'LATE_PAYMENT'
        end as fee_type,
        
        case 
            when series_value % 6 = 1 then 15.00
            when series_value % 6 = 2 then 35.00
            when series_value % 6 = 3 then 3.50
            when series_value % 6 = 4 then 25.00
            when series_value % 6 = 5 then (series_value % 50) + 5.00
            else 25.00
        end as fee_amount,
        
        '2024-01-01'::date + (series_value % 365) * interval '1 day' as fee_date,
        
        case 
            when series_value % 20 = 1 then 'WAIVED'
            when series_value % 50 = 1 then 'REFUNDED'
            else 'CHARGED'
        end as fee_status,
        
        current_timestamp as last_updated
        
    from generate_series(1, ({{ var('num_customers') }} * {{ var('num_accounts_multiplier') }} * 8)::int) as series_value
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
            when mf.fee_type in ('OVERDRAFT', 'LATE_PAYMENT') then 'PENALTY'
            when mf.fee_type = 'MONTHLY_MAINTENANCE' then 'SERVICE'
            else 'TRANSACTION'
        end as fee_classification,
        
        current_timestamp as dbt_created_at
        
    from mock_fees mf
)

select * from enriched_fees 
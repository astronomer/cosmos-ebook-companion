{{ config(materialized='view', tags=['bronze', 'staging', 'payments']) }}

with mock_payments as (
    select
        series_value as payment_id,
        'PAY' || lpad(series_value::text, 10, '0') as payment_number,
        ((series_value - 1) % ({{ var('num_customers') }} * {{ var('num_accounts_multiplier') }})::int) + 1 as account_id,
        
        case 
            when series_value % 5 = 1 then 'BILL_PAY'
            when series_value % 5 = 2 then 'PERSON_TO_PERSON'
            when series_value % 5 = 3 then 'MERCHANT_PAYMENT'
            when series_value % 5 = 4 then 'LOAN_PAYMENT'
            else 'TAX_PAYMENT'
        end as payment_type,
        
        (25 + (series_value % 2975))::numeric(10,2) as payment_amount,
        '2024-01-01'::date + (series_value % 365) * interval '1 day' as payment_date,
        
        case 
            when series_value % 50 = 1 then 'FAILED'
            when series_value % 25 = 1 then 'PENDING'
            else 'COMPLETED'
        end as payment_status,
        
        current_timestamp as last_updated
        
    from generate_series(1, ({{ var('num_customers') }} * {{ var('num_accounts_multiplier') }} * 25)::int) as series_value
)

select *, current_timestamp as dbt_created_at from mock_payments 
{{ config(materialized='view', tags=['bronze', 'staging', 'payments']) }}

with mock_payments as (
    select
        ROW_NUMBER() OVER (ORDER BY NULL) as payment_id,
        ((ROW_NUMBER() OVER (ORDER BY NULL) - 1) % ({{ var('num_customers') }} * {{ var('num_accounts_multiplier') }})::int) + 1 as account_id,
        
        case 
            when ROW_NUMBER() OVER (ORDER BY NULL) % 5 = 1 then 'CREDIT_CARD'
            when ROW_NUMBER() OVER (ORDER BY NULL) % 5 = 2 then 'DEBIT_CARD'
            when ROW_NUMBER() OVER (ORDER BY NULL) % 5 = 3 then 'ACH'
            when ROW_NUMBER() OVER (ORDER BY NULL) % 5 = 4 then 'WIRE_TRANSFER'
            else 'CHECK'
        end as payment_method,
        
        DATEADD(day, (ROW_NUMBER() OVER (ORDER BY NULL) % 365), '2024-01-01'::date) as payment_date,
        
        (10.00 + (ROW_NUMBER() OVER (ORDER BY NULL) % 2490))::numeric(10,2) as payment_amount,
        
        case 
            when ROW_NUMBER() OVER (ORDER BY NULL) % 20 = 1 then 'FAILED'
            when ROW_NUMBER() OVER (ORDER BY NULL) % 10 = 1 then 'PENDING'
            else 'COMPLETED'
        end as payment_status,
        
        current_timestamp as last_updated
        
    from TABLE(GENERATOR(ROWCOUNT => ({{ var('num_customers') }} * {{ var('num_accounts_multiplier') }} * 20)::int))
)

select *, current_timestamp as dbt_created_at from mock_payments 
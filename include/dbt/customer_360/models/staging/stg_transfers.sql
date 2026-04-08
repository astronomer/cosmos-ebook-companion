{{ config(materialized='view', tags=['bronze', 'staging', 'transfers']) }}

with mock_transfers as (
    select
        series_value as transfer_id,
        ((series_value - 1) % ({{ var('num_customers') }} * {{ var('num_accounts_multiplier') }})::int) + 1 as from_account_id,
        (((series_value + 3) - 1) % ({{ var('num_customers') }} * {{ var('num_accounts_multiplier') }})::int) + 1 as to_account_id,
        
        (50 + (series_value % 4950))::numeric(10,2) as transfer_amount,
        '2024-01-01'::date + (series_value % 365) * interval '1 day' as transfer_date,
        
        case 
            when series_value % 30 = 1 then 'FAILED'
            else 'COMPLETED'
        end as transfer_status,
        
        current_timestamp as last_updated
        
    from generate_series(1, ({{ var('num_customers') }} * {{ var('num_accounts_multiplier') }} * 5)::int) as series_value
)

select *, current_timestamp as dbt_created_at from mock_transfers 
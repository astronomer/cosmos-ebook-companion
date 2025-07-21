{{ config(materialized='view', tags=['bronze', 'staging', 'interest']) }}

with mock_interest as (
    select
        series_value as accrual_id,
        ((series_value - 1) % ({{ var('num_customers') }} * {{ var('num_accounts_multiplier') }})::int) + 1 as account_id,
        
        (1 + (series_value % 500))::numeric(8,2) as interest_amount,
        (1.5 + (series_value % 35) / 10.0) as interest_rate,
        
        '2024-01-01'::date + (series_value % 365) * interval '1 day' as accrual_date,
        
        current_timestamp as last_updated
        
    from generate_series(1, ({{ var('num_customers') }} * {{ var('num_accounts_multiplier') }} * 12)::int) as series_value
)

select *, current_timestamp as dbt_created_at from mock_interest 
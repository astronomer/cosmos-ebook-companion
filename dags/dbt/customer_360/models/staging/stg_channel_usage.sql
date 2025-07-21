{{ config(materialized='view', tags=['bronze', 'staging', 'channels']) }}

with mock_usage as (
    select
        series_value as usage_id,
        ((series_value - 1) % {{ var('num_customers') }}) + 1 as customer_id,
        
        case 
            when series_value % 5 = 1 then 'ONLINE'
            when series_value % 5 = 2 then 'MOBILE'
            when series_value % 5 = 3 then 'ATM'
            when series_value % 5 = 4 then 'BRANCH'
            else 'PHONE'
        end as channel_type,
        
        (1 + (series_value % 50)) as usage_count,
        '2024-01-01'::date + (series_value % 365) * interval '1 day' as usage_date,
        
        current_timestamp as last_updated
        
    from generate_series(1, ({{ var('num_customers') }} * 10)::int) as series_value
)

select *, current_timestamp as dbt_created_at from mock_usage 
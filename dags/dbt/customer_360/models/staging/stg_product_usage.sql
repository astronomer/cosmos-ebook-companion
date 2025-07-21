{{ config(materialized='view', tags=['bronze', 'staging', 'products']) }}

with mock_product_usage as (
    select
        series_value as usage_id,
        ((series_value - 1) % {{ var('num_customers') }}) + 1 as customer_id,
        
        case 
            when series_value % 8 = 1 then 'CHECKING'
            when series_value % 8 = 2 then 'SAVINGS'
            when series_value % 8 = 3 then 'CREDIT_CARD'
            when series_value % 8 = 4 then 'MORTGAGE'
            when series_value % 8 = 5 then 'INVESTMENTS'
            when series_value % 8 = 6 then 'INSURANCE'
            when series_value % 8 = 7 then 'LOANS'
            else 'MOBILE_BANKING'
        end as product_type,
        
        (1 + (series_value % 100)) as usage_frequency,
        '2024-01-01'::date + (series_value % 365) * interval '1 day' as usage_date,
        
        current_timestamp as last_updated
        
    from generate_series(1, ({{ var('num_customers') }} * 8)::int) as series_value
)

select *, current_timestamp as dbt_created_at from mock_product_usage 
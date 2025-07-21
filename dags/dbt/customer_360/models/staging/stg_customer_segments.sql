{{ config(materialized='view', tags=['bronze', 'staging', 'segments']) }}

with mock_segments as (
    select
        series_value as segment_id,
        ((series_value - 1) % {{ var('num_customers') }}) + 1 as customer_id,
        
        case 
            when series_value % 6 = 1 then 'PREMIUM'
            when series_value % 6 = 2 then 'MASS_AFFLUENT'
            when series_value % 6 = 3 then 'EMERGING_AFFLUENT'
            when series_value % 6 = 4 then 'MASS_MARKET'
            when series_value % 6 = 5 then 'YOUNG_PROFESSIONAL'
            else 'STUDENT'
        end as segment_name,
        
        '2024-01-01'::date + (series_value % 365) * interval '1 day' as segment_date,
        
        current_timestamp as last_updated
        
    from generate_series(1, ({{ var('num_customers') }} * 2)::int) as series_value
)

select *, current_timestamp as dbt_created_at from mock_segments 
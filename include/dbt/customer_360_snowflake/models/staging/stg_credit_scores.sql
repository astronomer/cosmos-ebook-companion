{{ config(
    materialized='view',
    tags=['bronze', 'staging', 'credit']
) }}

with mock_scores as (
    select
        ROW_NUMBER() OVER (ORDER BY NULL) as score_id,
        ((ROW_NUMBER() OVER (ORDER BY NULL) - 1) % {{ var('num_customers') }}) + 1 as customer_id,
        
        (300 + (ROW_NUMBER() OVER (ORDER BY NULL) % 550)) as score_value,
        case 
            when (300 + (ROW_NUMBER() OVER (ORDER BY NULL) % 550)) >= 800 then 'EXCELLENT'
            when (300 + (ROW_NUMBER() OVER (ORDER BY NULL) % 550)) >= 740 then 'VERY_GOOD'
            when (300 + (ROW_NUMBER() OVER (ORDER BY NULL) % 550)) >= 670 then 'GOOD'
            when (300 + (ROW_NUMBER() OVER (ORDER BY NULL) % 550)) >= 580 then 'FAIR'
            else 'POOR'
        end as score_grade,
        
        DATEADD(day, -(ROW_NUMBER() OVER (ORDER BY NULL) % 1095), '2024-01-01'::date) as score_date,
        
        case 
            when ROW_NUMBER() OVER (ORDER BY NULL) % 10 = 1 then 'EXPERIAN'
            when ROW_NUMBER() OVER (ORDER BY NULL) % 10 = 2 then 'EQUIFAX'
            else 'TRANSUNION'
        end as bureau,
        
        current_timestamp as last_updated
        
    from TABLE(GENERATOR(ROWCOUNT => ({{ var('num_customers') }} * 12)::int))
)

select *, current_timestamp as dbt_created_at from mock_scores 
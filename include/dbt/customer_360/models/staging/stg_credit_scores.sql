{{ config(
    materialized='view',
    tags=['bronze', 'staging', 'credit', 'history']
) }}

with mock_credit_scores as (
    select
        series_value as score_id,
        ((series_value - 1) % {{ var('num_customers') }}) + 1 as customer_id,
        
        (300 + (series_value % 550)) as credit_score,
        
        '2024-01-01'::date - (series_value % 1095) * interval '1 day' as score_date,
        
        case 
            when series_value % 3 = 1 then 'EXPERIAN'
            when series_value % 3 = 2 then 'EQUIFAX'
            else 'TRANSUNION'
        end as bureau,
        
        current_timestamp as last_updated
        
    from generate_series(1, ({{ var('num_customers') }} * 12)::int) as series_value
),

enriched_scores as (
    select
        mcs.*,
        
        case 
            when mcs.credit_score >= 800 then 'EXCEPTIONAL'
            when mcs.credit_score >= 740 then 'VERY_GOOD'
            when mcs.credit_score >= 670 then 'GOOD'
            when mcs.credit_score >= 580 then 'FAIR'
            else 'POOR'
        end as score_category,
        
        current_timestamp as dbt_created_at
        
    from mock_credit_scores mcs
)

select * from enriched_scores 
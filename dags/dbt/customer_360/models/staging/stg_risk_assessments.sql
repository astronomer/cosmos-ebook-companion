{{ config(
    materialized='view',
    tags=['bronze', 'staging', 'risk', 'compliance']
) }}

with mock_assessments as (
    select
        series_value as assessment_id,
        'RISK' || lpad(series_value::text, 8, '0') as assessment_number,
        ((series_value - 1) % {{ var('num_customers') }}) + 1 as customer_id,
        
        case 
            when series_value % 4 = 1 then 'CREDIT_RISK'
            when series_value % 4 = 2 then 'FRAUD_RISK' 
            when series_value % 4 = 3 then 'OPERATIONAL_RISK'
            else 'MARKET_RISK'
        end as risk_type,
        
        (300 + (series_value % 550)) as risk_score,
        
        case 
            when (300 + (series_value % 550)) >= 750 then 'LOW'
            when (300 + (series_value % 550)) >= 650 then 'MEDIUM'
            when (300 + (series_value % 550)) >= 550 then 'HIGH'
            else 'VERY_HIGH'
        end as risk_rating,
        
        '2024-01-01'::date + (series_value % 365) * interval '1 day' as assessment_date,
        
        case when series_value % 10 = 1 then true else false end as requires_review,
        
        current_timestamp as last_updated
        
    from generate_series(1, ({{ var('num_customers') }} * 4)::int) as series_value
),

enriched_assessments as (
    select
        ma.*,
        
        case 
            when ma.risk_score >= 800 then 'EXCELLENT'
            when ma.risk_score >= 740 then 'VERY_GOOD'
            when ma.risk_score >= 670 then 'GOOD'
            when ma.risk_score >= 580 then 'FAIR'
            else 'POOR'
        end as score_category,
        
        current_timestamp as dbt_created_at
        
    from mock_assessments ma
)

select * from enriched_assessments 
{{ config(
    materialized='view',
    tags=['bronze', 'staging', 'insurance', 'coverage']
) }}

with mock_policies as (
    select
        series_value as policy_id,
        'POL' || lpad(series_value::text, 8, '0') as policy_number,
        ((series_value - 1) % {{ var('num_customers') }}) + 1 as customer_id,
        
        case 
            when series_value % 4 = 1 then 'LIFE'
            when series_value % 4 = 2 then 'AUTO'
            when series_value % 4 = 3 then 'HOME'
            else 'HEALTH'
        end as policy_type,
        
        case 
            when series_value % 4 = 1 then (50000 + (series_value % 950000))::numeric(12,2)  -- Life coverage
            when series_value % 4 = 2 then (25000 + (series_value % 75000))::numeric(12,2)   -- Auto coverage
            when series_value % 4 = 3 then (200000 + (series_value % 800000))::numeric(12,2) -- Home coverage
            else (5000 + (series_value % 45000))::numeric(12,2)                              -- Health coverage
        end as coverage_amount,
        
        case 
            when series_value % 4 = 1 then (200 + (series_value % 1800))::numeric(8,2)  -- Life premium
            when series_value % 4 = 2 then (800 + (series_value % 2200))::numeric(8,2)  -- Auto premium
            when series_value % 4 = 3 then (1200 + (series_value % 2800))::numeric(8,2) -- Home premium
            else (400 + (series_value % 1600))::numeric(8,2)                             -- Health premium
        end as annual_premium,
        
        '2020-01-01'::date + (series_value % 1460) * interval '1 day' as policy_start_date,
        
        case 
            when series_value % 20 = 1 then 'CANCELLED'
            when series_value % 15 = 1 then 'LAPSED'
            when series_value % 10 = 1 then 'SUSPENDED'
            else 'ACTIVE'
        end as policy_status,
        
        (series_value % 5) as claims_count,
        
        current_timestamp as last_updated
        
    from generate_series(1, ({{ var('num_customers') }} * 0.8)::int) as series_value
),

enriched_policies as (
    select
        mp.*,
        mp.policy_start_date + interval '1 year' as policy_end_date,
        date_part('year', age(current_date, mp.policy_start_date))::int as policy_age_years,
        
        case 
            when mp.claims_count = 0 then 'NO_CLAIMS'
            when mp.claims_count <= 2 then 'LOW_CLAIMS'
            when mp.claims_count <= 4 then 'MODERATE_CLAIMS'
            else 'HIGH_CLAIMS'
        end as claims_category,
        
        case 
            when mp.annual_premium > 0 and mp.coverage_amount > 0 then 
                round((mp.annual_premium / mp.coverage_amount * 100)::numeric, 4)
            else 0
        end as premium_rate_percent,
        
        current_timestamp as dbt_created_at
        
    from mock_policies mp
)

select * from enriched_policies 
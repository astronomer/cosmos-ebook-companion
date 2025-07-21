{{ config(
    materialized='view',
    tags=['bronze', 'staging', 'investments', 'wealth']
) }}

/*
    Staging model for investment holdings
    
    Tracks portfolio holdings across asset classes
    with performance and risk analytics.
*/

with mock_investments as (
    select
        ROW_NUMBER() OVER (ORDER BY NULL) as investment_id,
        ((ROW_NUMBER() OVER (ORDER BY NULL) - 1) % {{ var('num_customers') }}) + 1 as customer_id,
        
        case 
            when ROW_NUMBER() OVER (ORDER BY NULL) % 4 = 1 then 'STOCKS'
            when ROW_NUMBER() OVER (ORDER BY NULL) % 4 = 2 then 'BONDS'
            when ROW_NUMBER() OVER (ORDER BY NULL) % 4 = 3 then 'MUTUAL_FUNDS'
            else 'ETF'
        end as investment_type,
        
        DATEADD(day, -(ROW_NUMBER() OVER (ORDER BY NULL) % 1095), '2024-01-01'::date) as purchase_date,
        
        (1000 + (ROW_NUMBER() OVER (ORDER BY NULL) % 49000))::numeric(12,2) as investment_amount,
        
        case 
            when ROW_NUMBER() OVER (ORDER BY NULL) % 10 = 1 then 'SOLD'
            else 'ACTIVE'
        end as investment_status,
        
        current_timestamp as last_updated
        
    from TABLE(GENERATOR(ROWCOUNT => ({{ var('num_customers') }} * 5)::int))
),

base_investments as (
    select
        investment_id,
        customer_id,
        
        -- Investment details
        UPPER(investment_type) as investment_type,
        investment_amount,
        investment_status,
        purchase_date,
        
        -- Current market values
        case 
            when investment_type = 'STOCKS' then round((90 + (ROW_NUMBER() OVER (ORDER BY NULL) % 920))::numeric, 2)
            when investment_type = 'BONDS' then round((93 + (ROW_NUMBER() OVER (ORDER BY NULL) % 15))::numeric, 2)
            when investment_type = 'MUTUAL_FUNDS' then round((14 + (ROW_NUMBER() OVER (ORDER BY NULL) % 90))::numeric, 2)
            when investment_type = 'ETF' then round((48 + (ROW_NUMBER() OVER (ORDER BY NULL) % 460))::numeric, 2)
            else 1.0
        end as current_price,
        
        -- Performance calculations
        case 
            when investment_type = 'STOCKS' then round(((current_price - investment_amount) / investment_amount * 100)::numeric, 2)
            else 0
        end as return_percent,
        
        -- Time calculations
        DATEDIFF(year, purchase_date, current_date)::int as holding_years,
        
        last_updated,
        current_timestamp as dbt_created_at
        
    from mock_investments
),

enriched_investments as (
    select
        bi.*,
        
        -- Asset classification
        case 
            when bi.investment_type in ('STOCKS', 'ETF') then 'EQUITY'
            when bi.investment_type = 'BONDS' then 'FIXED_INCOME'
            when bi.investment_type = 'MUTUAL_FUNDS' then 'POOLED_INVESTMENT'
            else 'OTHER'
        end as asset_class,
        
        -- Performance categories
        case 
            when bi.return_percent >= 20 then 'STRONG_PERFORMER'
            when bi.return_percent >= 10 then 'GOOD_PERFORMER'
            when bi.return_percent >= 0 then 'MODERATE_PERFORMER'
            when bi.return_percent >= -10 then 'POOR_PERFORMER'
            else 'SIGNIFICANT_LOSS'
        end as performance_category,
        
        -- Size categories
        case 
            when bi.investment_amount >= 100000 then 'LARGE_HOLDING'
            when bi.investment_amount >= 25000 then 'MEDIUM_HOLDING'
            when bi.investment_amount >= 5000 then 'SMALL_HOLDING'
            else 'MINIMAL_HOLDING'
        end as holding_size,
        
        -- Tax implications
        case 
            when bi.investment_type in ('STOCKS', 'BONDS') and (bi.current_price - bi.investment_amount) > 0 then 'TAXABLE_GAIN'
            when bi.investment_type in ('STOCKS', 'BONDS') and (bi.current_price - bi.investment_amount) < 0 then 'TAX_LOSS_HARVEST'
            else 'NO_TAX_IMPACT'
        end as tax_status,
        
        -- Risk-adjusted metrics
        case 
            when bi.investment_type = 'STOCKS' and bi.return_percent > 15 then 'HIGH_RISK_HIGH_RETURN'
            when bi.investment_type = 'STOCKS' and bi.return_percent < 0 then 'HIGH_RISK_POOR_RETURN'
            when bi.investment_type = 'BONDS' and bi.return_percent > 5 then 'LOW_RISK_GOOD_RETURN'
            when bi.investment_type = 'BONDS' and bi.return_percent < 0 then 'LOW_RISK_POOR_RETURN'
            else 'MODERATE_RISK_RETURN'
        end as risk_return_profile,
        
        -- Liquidity assessment
        case 
            when bi.investment_type in ('STOCKS', 'ETF') then 'HIGH_LIQUIDITY'
            when bi.investment_type = 'MUTUAL_FUNDS' then 'MEDIUM_LIQUIDITY'
            when bi.investment_type = 'BONDS' then 'MEDIUM_LIQUIDITY'
            else 'HIGH_LIQUIDITY'
        end as liquidity_level,
        
        -- Investment tenure
        case 
            when bi.holding_years >= 5 then 'LONG_TERM'
            when bi.holding_years >= 2 then 'MEDIUM_TERM'
            when bi.holding_years >= 1 then 'SHORT_TERM'
            else 'VERY_SHORT_TERM'
        end as tenure_category,
        
        -- Portfolio flags
        case 
            when bi.investment_amount >= 50000 then true 
            else false 
        end as is_major_holding,
        
        case 
            when bi.return_percent < -20 then true 
            else false 
        end as requires_review
        
    from base_investments bi
)

select * from enriched_investments 
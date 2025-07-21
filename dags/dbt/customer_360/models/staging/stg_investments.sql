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
        series_value as holding_id,
        'INV' || lpad(series_value::text, 8, '0') as holding_number,
        
        -- Link to customers (subset have investments)
        ((series_value - 1) % ({{ var('num_customers') }} * 0.4)::int) + 1 as customer_id,
        
        -- Investment types
        case 
            when series_value % 6 = 1 then 'STOCK'
            when series_value % 6 = 2 then 'BOND'
            when series_value % 6 = 3 then 'MUTUAL_FUND'
            when series_value % 6 = 4 then 'ETF'
            when series_value % 6 = 5 then 'CD'
            else 'MONEY_MARKET'
        end as investment_type,
        
        -- Asset symbols/names
        case 
            when series_value % 6 = 1 then  -- Stocks
                case 
                    when series_value % 10 = 1 then 'AAPL'
                    when series_value % 10 = 2 then 'MSFT'
                    when series_value % 10 = 3 then 'GOOGL'
                    when series_value % 10 = 4 then 'AMZN'
                    when series_value % 10 = 5 then 'TSLA'
                    else 'SPY'
                end
            when series_value % 6 = 2 then 'US_TREASURY_10YR'  -- Bonds
            when series_value % 6 = 3 then 'VANGUARD_500'      -- Mutual funds
            when series_value % 6 = 4 then 'VTI'               -- ETFs
            when series_value % 6 = 5 then 'BANK_CD_5YR'       -- CDs
            else 'MONEY_MARKET_FUND'                            -- Money market
        end as symbol,
        
        -- Holdings
        case 
            when series_value % 6 in (1, 4) then (series_value % 1000) + 1     -- Shares for stocks/ETFs
            when series_value % 6 = 2 then (series_value % 100000) + 1000      -- Bond face value
            when series_value % 6 = 3 then round((series_value % 50000 + 1000)::numeric, 2)  -- Mutual fund shares
            when series_value % 6 = 5 then (series_value % 100000) + 5000      -- CD amounts
            else round((series_value % 50000 + 1000)::numeric, 2)              -- Money market shares
        end as quantity,
        
        -- Purchase info
        case 
            when series_value % 6 = 1 then round((100 + (series_value % 900))::numeric, 2)     -- Stock prices
            when series_value % 6 = 2 then round((95 + (series_value % 10))::numeric, 2)       -- Bond prices
            when series_value % 6 = 3 then round((15 + (series_value % 85))::numeric, 2)       -- Mutual fund NAV
            when series_value % 6 = 4 then round((50 + (series_value % 450))::numeric, 2)      -- ETF prices
            when series_value % 6 = 5 then 1.0                                                  -- CD (par value)
            else 1.0                                                                             -- Money market
        end as purchase_price,
        
        -- Current market values
        case 
            when series_value % 6 = 1 then round((90 + (series_value % 920))::numeric, 2)      -- Stock current
            when series_value % 6 = 2 then round((93 + (series_value % 15))::numeric, 2)       -- Bond current
            when series_value % 6 = 3 then round((14 + (series_value % 90))::numeric, 2)       -- Mutual fund current
            when series_value % 6 = 4 then round((48 + (series_value % 460))::numeric, 2)      -- ETF current
            when series_value % 6 = 5 then 1.0                                                  -- CD (stable)
            else 1.0                                                                             -- Money market
        end as current_price,
        
        -- Purchase date
        '2020-01-01'::date + (series_value % 1460) * interval '1 day' as purchase_date,
        
        -- Account types
        case 
            when series_value % 4 = 1 then 'TAXABLE'
            when series_value % 4 = 2 then 'IRA'
            when series_value % 4 = 3 then 'ROTH_IRA'
            else '401K'
        end as account_type,
        
        -- Risk ratings
        case 
            when series_value % 6 = 1 then  -- Stocks
                case when series_value % 3 = 1 then 'HIGH' else 'MEDIUM' end
            when series_value % 6 = 2 then 'LOW'        -- Bonds
            when series_value % 6 = 3 then 'MEDIUM'     -- Mutual funds
            when series_value % 6 = 4 then 'MEDIUM'     -- ETFs
            else 'LOW'                                   -- CDs/Money market
        end as risk_rating,
        
        current_timestamp as last_updated
        
    from generate_series(1, ({{ var('num_customers') }} * 0.4 * 12)::int) as series_value
),

base_investments as (
    select
        holding_id,
        holding_number,
        customer_id,
        
        -- Investment details
        upper(investment_type) as investment_type,
        upper(symbol) as symbol,
        quantity,
        purchase_price,
        current_price,
        purchase_date,
        upper(account_type) as account_type,
        upper(risk_rating) as risk_rating,
        
        -- Calculated values
        round((quantity * purchase_price)::numeric, 2) as purchase_value,
        round((quantity * current_price)::numeric, 2) as current_value,
        round((quantity * current_price - quantity * purchase_price)::numeric, 2) as unrealized_gain_loss,
        
        -- Performance calculations
        case 
            when purchase_price > 0 then 
                round(((current_price - purchase_price) / purchase_price * 100)::numeric, 2)
            else 0
        end as return_percent,
        
        -- Time calculations
        date_part('year', age(current_date, purchase_date))::int as holding_years,
        
        last_updated,
        current_timestamp as dbt_created_at
        
    from mock_investments
),

enriched_investments as (
    select
        bi.*,
        
        -- Asset classification
        case 
            when bi.investment_type in ('STOCK', 'ETF') then 'EQUITY'
            when bi.investment_type = 'BOND' then 'FIXED_INCOME'
            when bi.investment_type = 'MUTUAL_FUND' then 'POOLED_INVESTMENT'
            when bi.investment_type in ('CD', 'MONEY_MARKET') then 'CASH_EQUIVALENT'
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
            when bi.current_value >= 100000 then 'LARGE_HOLDING'
            when bi.current_value >= 25000 then 'MEDIUM_HOLDING'
            when bi.current_value >= 5000 then 'SMALL_HOLDING'
            else 'MINIMAL_HOLDING'
        end as holding_size,
        
        -- Tax implications
        case 
            when bi.account_type = 'TAXABLE' and bi.unrealized_gain_loss > 0 then 'TAXABLE_GAIN'
            when bi.account_type = 'TAXABLE' and bi.unrealized_gain_loss < 0 then 'TAX_LOSS_HARVEST'
            when bi.account_type in ('IRA', 'ROTH_IRA', '401K') then 'TAX_DEFERRED'
            else 'NO_TAX_IMPACT'
        end as tax_status,
        
        -- Risk-adjusted metrics
        case 
            when bi.risk_rating = 'HIGH' and bi.return_percent > 15 then 'HIGH_RISK_HIGH_RETURN'
            when bi.risk_rating = 'HIGH' and bi.return_percent < 0 then 'HIGH_RISK_POOR_RETURN'
            when bi.risk_rating = 'LOW' and bi.return_percent > 5 then 'LOW_RISK_GOOD_RETURN'
            when bi.risk_rating = 'LOW' and bi.return_percent < 0 then 'LOW_RISK_POOR_RETURN'
            else 'MODERATE_RISK_RETURN'
        end as risk_return_profile,
        
        -- Liquidity assessment
        case 
            when bi.investment_type in ('STOCK', 'ETF') then 'HIGH_LIQUIDITY'
            when bi.investment_type = 'MUTUAL_FUND' then 'MEDIUM_LIQUIDITY'
            when bi.investment_type = 'BOND' then 'MEDIUM_LIQUIDITY'
            when bi.investment_type = 'CD' then 'LOW_LIQUIDITY'
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
            when bi.current_value >= 50000 then true 
            else false 
        end as is_major_holding,
        
        case 
            when bi.return_percent < -20 then true 
            else false 
        end as requires_review
        
    from base_investments bi
)

select * from enriched_investments 
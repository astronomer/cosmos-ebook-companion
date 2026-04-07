{{ config(
    materialized='table',
    tags=['gold', 'mart', 'executive', 'financial']
) }}

/*
    Financial Performance Summary
    
    Executive-level financial performance dashboard combining:
    - Account profitability and performance metrics
    - Customer financial relationships and value
    - Transaction volumes and revenue streams
    - Product penetration and cross-sell performance
    - Risk-adjusted returns and portfolio health
    
    Provides CFO/CRO level insights for financial strategy and performance management.
*/

with account_financial_performance as (
    select
        apd.account_id,
        apd.customer_id,
        apd.product_type,
        apd.account_age_years,
        apd.current_balance,
        apd.annual_interest_rate,
        apd.monthly_fee,
        apd.account_profitability_score,
        apd.account_health_score,
        apd.total_transactions,
        apd.total_transaction_volume,
        apd.net_cash_flow,
        apd.total_fees_paid,
        apd.customer_relationship_value,
        apd.customer_wealth_tier,
        apd.overall_risk_level,
        apd.account_service_tier,
        
        -- Calculate revenue metrics
        (apd.monthly_fee * 12) as annual_fee_revenue,
        (apd.current_balance * apd.annual_interest_rate) as annual_interest_expense,
        ((apd.monthly_fee * 12) - (apd.current_balance * apd.annual_interest_rate)) as annual_net_revenue,
        apd.total_fees_paid as fee_income_generated,
        
        -- Risk-adjusted metrics
        case 
            when apd.overall_risk_level = 'LOW_RISK' then 1.0
            when apd.overall_risk_level = 'MODERATE_RISK' then 0.8
            when apd.overall_risk_level = 'ELEVATED_RISK' then 0.6
            when apd.overall_risk_level = 'HIGH_RISK' then 0.3
            else 0.1
        end as risk_adjustment_factor
        
    from {{ ref('int_account_performance_dashboard') }} apd
),

customer_financial_metrics as (
    select
        cfs.customer_id,
        cfs.total_relationship_value,
        cfs.net_worth_with_bank,
        cfs.product_penetration_score,
        cfs.wealth_tier,
        cfs.financial_risk_level,
        cfs.total_deposit_accounts,
        cfs.total_deposit_balances,
        cfs.total_cards,
        cfs.total_credit_limit,
        cfs.total_loans,
        cfs.total_loan_balance,
        cfs.total_investments,
        cfs.total_investment_value,
        
        -- Cross-sell opportunities
        ppa.cross_sell_opportunity_score,
        ppa.product_stickiness_score,
        ppa.next_best_product,
        
        -- Customer profile context
        cp.relationship_years,
        cp.customer_value_segment,
        cp.engagement_level
        
    from {{ ref('int_customer_financial_summary') }} cfs
    left join {{ ref('int_product_penetration_analysis') }} ppa on cfs.customer_id = ppa.customer_id
    left join {{ ref('int_customer_profile') }} cp on cfs.customer_id = cp.customer_id
),

financial_portfolio_summary as (
    select
        -- Portfolio Metrics
        count(distinct afp.account_id) as total_accounts,
        count(distinct afp.customer_id) as total_customers,
        count(distinct case when afp.account_service_tier = 'PRIVATE_BANKING' then afp.customer_id end) as private_banking_customers,
        count(distinct case when afp.customer_wealth_tier = 'WEALTH_MANAGEMENT' then afp.customer_id end) as wealth_management_customers,
        
        -- Asset Metrics
        sum(afp.current_balance) as total_assets_under_management,
        avg(afp.current_balance) as avg_account_balance,
        sum(case when afp.customer_wealth_tier = 'PRIVATE_BANKING' then afp.current_balance else 0 end) as private_banking_assets,
        sum(case when afp.product_type = 'INVESTMENT' then afp.current_balance else 0 end) as investment_assets,
        
        -- Revenue Metrics
        sum(afp.annual_fee_revenue) as total_annual_fee_revenue,
        sum(afp.annual_interest_expense) as total_annual_interest_expense,
        sum(afp.annual_net_revenue) as total_annual_net_revenue,
        sum(afp.fee_income_generated) as total_fee_income,
        avg(afp.account_profitability_score) as avg_account_profitability,
        
        -- Performance Metrics
        sum(afp.total_transaction_volume) as total_transaction_volume,
        avg(afp.account_health_score) as avg_account_health,
        sum(case when afp.account_health_score >= 80 then afp.current_balance else 0 end) as healthy_account_assets,
        
        -- Risk Metrics
        sum(afp.current_balance * afp.risk_adjustment_factor) as risk_adjusted_assets,
        sum(case when afp.overall_risk_level in ('HIGH_RISK', 'CRITICAL_RISK') then afp.current_balance else 0 end) as high_risk_assets,
        avg(afp.risk_adjustment_factor) as avg_risk_adjustment,
        
        -- Activity Metrics
        sum(afp.total_transactions) as total_transactions,
        sum(case when afp.total_transactions > 0 then 1 else 0 end) as active_accounts,
        sum(case when afp.total_transactions = 0 and afp.account_age_years > 1 then 1 else 0 end) as dormant_accounts
        
    from account_financial_performance afp
),

customer_portfolio_analysis as (
    select
        -- Customer Value Distribution
        count(*) as total_customers,
        count(case when cfm.customer_value_segment = 'High Value' then 1 end) as high_value_customers,
        sum(cfm.total_relationship_value) as total_customer_value,
        avg(cfm.total_relationship_value) as avg_customer_value,
        
        -- Wealth Tier Analysis
        sum(case when cfm.wealth_tier = 'PRIVATE_BANKING' then cfm.total_relationship_value else 0 end) as private_banking_value,
        sum(case when cfm.wealth_tier = 'WEALTH_MANAGEMENT' then cfm.total_relationship_value else 0 end) as wealth_management_value,
        sum(case when cfm.wealth_tier = 'PREFERRED' then cfm.total_relationship_value else 0 end) as preferred_value,
        
        -- Product Penetration
        avg(cfm.product_penetration_score) as avg_product_penetration,
        avg(cfm.total_deposit_accounts) as avg_deposit_accounts_per_customer,
        sum(cfm.total_deposit_balances) as total_deposit_balances,
        sum(cfm.total_loan_balance) as total_loan_balances,
        sum(cfm.total_investment_value) as total_investment_value,
        
        -- Cross-sell Potential
        count(case when cfm.cross_sell_opportunity_score >= 75 then 1 end) as high_cross_sell_potential,
        avg(cfm.cross_sell_opportunity_score) as avg_cross_sell_score,
        avg(cfm.product_stickiness_score) as avg_product_stickiness,
        
        -- Relationship Metrics
        avg(cfm.relationship_years) as avg_relationship_tenure,
        count(case when cfm.engagement_level = 'HIGHLY_ENGAGED' then 1 end) as highly_engaged_customers
        
    from customer_financial_metrics cfm
),

profitability_analysis as (
    select
        fps.total_annual_net_revenue / nullif(fps.total_customers, 0) as revenue_per_customer,
        fps.total_annual_fee_revenue / nullif(fps.total_accounts, 0) as fee_revenue_per_account,
        fps.total_assets_under_management / nullif(fps.total_customers, 0) as assets_per_customer,
        fps.healthy_account_assets / nullif(fps.total_assets_under_management, 0) * 100 as healthy_assets_percentage,
        fps.risk_adjusted_assets / nullif(fps.total_assets_under_management, 0) * 100 as risk_adjusted_assets_percentage,
        fps.active_accounts / nullif(fps.total_accounts, 0) * 100 as account_activation_rate,
        fps.private_banking_assets / nullif(fps.total_assets_under_management, 0) * 100 as private_banking_share,
        
        -- Performance ratios
        fps.total_annual_net_revenue / nullif(fps.total_assets_under_management, 0) * 100 as net_revenue_yield,
        fps.total_fee_income / nullif(fps.total_transaction_volume, 0) * 100 as fee_rate_on_volume,
        fps.dormant_accounts / nullif(fps.total_accounts, 0) * 100 as dormancy_rate
        
    from financial_portfolio_summary fps
)

select
    'FINANCIAL_OVERVIEW' as dashboard_section,
    current_date as report_date,
    
    -- Portfolio Size & Scale
    fps.total_customers,
    fps.total_accounts,
    round(fps.total_assets_under_management::numeric, 0) as total_aum,
    round(fps.avg_account_balance::numeric, 0) as avg_account_balance,
    round(fps.private_banking_assets::numeric, 0) as private_banking_aum,
    round(fps.investment_assets::numeric, 0) as investment_aum,
    
    -- Revenue Performance
    round(fps.total_annual_net_revenue::numeric, 0) as annual_net_revenue,
    round(fps.total_annual_fee_revenue::numeric, 0) as annual_fee_revenue,
    round(fps.total_fee_income::numeric, 0) as transaction_fee_income,
    round(pa.revenue_per_customer::numeric, 0) as revenue_per_customer,
    round(pa.fee_revenue_per_account::numeric, 0) as fee_revenue_per_account,
    
    -- Performance Metrics
    round(fps.avg_account_profitability::numeric, 1) as avg_account_profitability_score,
    round(fps.avg_account_health::numeric, 1) as avg_account_health_score,
    round(pa.healthy_assets_percentage::numeric, 1) as healthy_assets_percentage,
    round(pa.account_activation_rate::numeric, 1) as account_activation_rate,
    
    -- Risk & Quality
    round(pa.risk_adjusted_assets_percentage::numeric, 1) as risk_adjusted_assets_pct,
    round(fps.avg_risk_adjustment::numeric, 2) as avg_risk_adjustment_factor,
    round(pa.dormancy_rate::numeric, 1) as account_dormancy_rate,
    fps.dormant_accounts as dormant_accounts_count,
    
    -- Customer Value
    round(cpa.avg_customer_value::numeric, 0) as avg_customer_relationship_value,
    round(cpa.private_banking_value::numeric, 0) as private_banking_customer_value,
    round(cpa.wealth_management_value::numeric, 0) as wealth_management_customer_value,
    cpa.high_value_customers,
    cpa.highly_engaged_customers,
    
    -- Product Performance
    round(cpa.avg_product_penetration::numeric, 1) as avg_product_penetration_score,
    round(cpa.avg_deposit_accounts_per_customer::numeric, 1) as avg_accounts_per_customer,
    cpa.high_cross_sell_potential as high_cross_sell_customers,
    round(cpa.avg_cross_sell_score::numeric, 1) as avg_cross_sell_opportunity_score,
    
    -- Growth Indicators
    round(pa.net_revenue_yield::numeric, 2) as net_revenue_yield_pct,
    round(pa.fee_rate_on_volume::numeric, 4) as fee_rate_on_transaction_volume,
    round(pa.private_banking_share::numeric, 1) as private_banking_share_pct,
    round(cpa.avg_relationship_tenure::numeric, 1) as avg_relationship_years,
    
    -- Strategic Insights
    case 
        when pa.net_revenue_yield >= 3.0 then 'HIGH_YIELD_PORTFOLIO'
        when pa.net_revenue_yield >= 2.0 then 'STRONG_YIELD'
        when pa.net_revenue_yield >= 1.0 then 'MODERATE_YIELD'
        else 'YIELD_IMPROVEMENT_NEEDED'
    end as portfolio_yield_assessment,
    
    case 
        when pa.healthy_assets_percentage >= 80 then 'EXCELLENT_PORTFOLIO_HEALTH'
        when pa.healthy_assets_percentage >= 60 then 'GOOD_PORTFOLIO_HEALTH'
        when pa.healthy_assets_percentage >= 40 then 'FAIR_PORTFOLIO_HEALTH'
        else 'PORTFOLIO_HEALTH_ATTENTION_NEEDED'
    end as portfolio_health_status,
    
    case 
        when cpa.avg_product_penetration >= 75 then 'EXCELLENT_CROSS_SELL'
        when cpa.avg_product_penetration >= 50 then 'GOOD_CROSS_SELL_PERFORMANCE'
        when cpa.avg_product_penetration >= 30 then 'MODERATE_CROSS_SELL'
        else 'CROSS_SELL_OPPORTUNITY'
    end as cross_sell_performance,
    
    -- Priority Actions
    fps.dormant_accounts as dormant_accounts_requiring_attention,
    cpa.high_cross_sell_potential as cross_sell_opportunities_available,
    case 
        when pa.dormancy_rate > 15 then 'ACTIVATION_CAMPAIGN_NEEDED'
        when pa.account_activation_rate < 85 then 'ENGAGEMENT_IMPROVEMENT_NEEDED'
        else 'MAINTAIN_CURRENT_STRATEGY'
    end as recommended_action,
    
    current_timestamp as last_updated

from financial_portfolio_summary fps
cross join customer_portfolio_analysis cpa  
cross join profitability_analysis pa 
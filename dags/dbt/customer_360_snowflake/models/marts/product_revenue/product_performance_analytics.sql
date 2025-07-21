{{ config(
    materialized='table',
    tags=['gold', 'mart', 'product', 'revenue']
) }}

/*
    Product Performance Analytics
    
    Comprehensive product performance and revenue analysis:
    - Product adoption rates and penetration metrics
    - Cross-sell and upsell opportunity analysis
    - Product profitability and revenue contribution
    - Customer lifecycle product journey mapping
    - Channel effectiveness for product sales
    
    Provides Chief Product Officer/Head of Products insights for
    product strategy, development priorities, and revenue optimization.
*/

with product_customer_analysis as (
    select
        ppa.customer_id,
        ppa.product_penetration_score,
        ppa.digital_adoption_level,
        ppa.marketing_responsiveness,
        ppa.cross_sell_opportunity_score,
        ppa.product_stickiness_score,
        ppa.next_best_product,
        
        -- Customer context
        cp.customer_value_segment,
        cp.lifecycle_stage_name,
        cp.engagement_level,
        cp.digital_preference,
        cp.relationship_years,
        cp.age,
        
        -- Financial context
        cfs.total_relationship_value,
        cfs.customer_value_segment as wealth_tier,
        cfs.total_deposit_accounts,
        cfs.total_cards,
        cfs.total_loans,
        cfs.total_investments,
        cfs.net_worth_with_bank,
        
        -- Account performance
        apd.account_profitability_score,
        apd.account_health_score,
        apd.product_type as primary_product_type,
        apd.account_service_tier,
        
        -- Channel effectiveness
        ce.digital_adoption_score,
        ce.channel_preference,
        ce.service_satisfaction_score,
        
        -- Retention context
        cra.churn_risk_score,
        cra.retention_opportunity_score
        
    from {{ ref('int_product_penetration_analysis') }} ppa
    left join {{ ref('int_customer_profile') }} cp on ppa.customer_id = cp.customer_id
    left join {{ ref('int_customer_financial_summary') }} cfs on ppa.customer_id = cfs.customer_id
    left join {{ ref('int_account_performance_dashboard') }} apd on ppa.customer_id = apd.customer_id
    left join {{ ref('int_channel_effectiveness') }} ce on ppa.customer_id = ce.customer_id
    left join {{ ref('int_customer_retention_analytics') }} cra on ppa.customer_id = cra.customer_id
),

product_portfolio_metrics as (
    select
        -- Customer Base Analysis
        count(*) as total_customers,
        count(case when total_deposit_accounts > 0 then 1 end) as deposit_account_customers,
        count(case when total_cards > 0 then 1 end) as card_customers,
        count(case when total_loans > 0 then 1 end) as loan_customers,
        count(case when total_investments > 0 then 1 end) as investment_customers,
        
        -- Product Penetration Rates
        round(count(case when total_deposit_accounts > 0 then 1 end)::numeric / count(*) * 100, 1) as deposit_penetration_rate,
        round(count(case when total_cards > 0 then 1 end)::numeric / count(*) * 100, 1) as card_penetration_rate,
        round(count(case when total_loans > 0 then 1 end)::numeric / count(*) * 100, 1) as loan_penetration_rate,
        round(count(case when total_investments > 0 then 1 end)::numeric / count(*) * 100, 1) as investment_penetration_rate,
        
        -- Multi-Product Customers
        count(case when (case when total_deposit_accounts > 0 then 1 else 0 end) + 
                       (case when total_cards > 0 then 1 else 0 end) + 
                       (case when total_loans > 0 then 1 else 0 end) + 
                       (case when total_investments > 0 then 1 else 0 end) >= 2 then 1 end) as multi_product_customers,
        count(case when (case when total_deposit_accounts > 0 then 1 else 0 end) + 
                       (case when total_cards > 0 then 1 else 0 end) + 
                       (case when total_loans > 0 then 1 else 0 end) + 
                       (case when total_investments > 0 then 1 else 0 end) >= 3 then 1 end) as premium_product_customers,
        
        -- Product Performance Scores
        avg(product_penetration_score) as avg_product_penetration_score,
        avg(cross_sell_opportunity_score) as avg_cross_sell_opportunity,
        avg(product_stickiness_score) as avg_product_stickiness,
        avg(account_profitability_score) as avg_account_profitability,
        
        -- Digital Product Adoption
        count(case when digital_adoption_level = 'DIGITAL_NATIVE' then 1 end) as digital_native_customers,
        count(case when digital_adoption_level = 'DIGITAL_ADOPTER' then 1 end) as digital_adopter_customers,
        count(case when digital_preference in ('DIGITAL_FIRST', 'DIGITAL_PREFERRED') then 1 end) as digital_preferred_customers,
        round(count(case when digital_preference in ('DIGITAL_FIRST', 'DIGITAL_PREFERRED') then 1 end)::numeric / count(*) * 100, 1) as digital_adoption_rate,
        
        -- Cross-sell Opportunities
        count(case when cross_sell_opportunity_score >= 75 then 1 end) as high_cross_sell_opportunity,
        count(case when cross_sell_opportunity_score >= 50 then 1 end) as medium_cross_sell_opportunity,
        
        -- Customer Value Analysis
        sum(total_relationship_value) as total_product_portfolio_value,
        avg(total_relationship_value) as avg_customer_relationship_value
        
    from product_customer_analysis
),

product_segment_analysis as (
    select
        customer_value_segment,
        count(*) as segment_customers,
        
        -- Product Adoption by Segment
        avg(product_penetration_score) as avg_penetration_score,
        round(count(case when total_deposit_accounts > 0 then 1 end)::numeric / count(*) * 100, 1) as deposit_adoption_rate,
        round(count(case when total_cards > 0 then 1 end)::numeric / count(*) * 100, 1) as card_adoption_rate,
        round(count(case when total_loans > 0 then 1 end)::numeric / count(*) * 100, 1) as loan_adoption_rate,
        round(count(case when total_investments > 0 then 1 end)::numeric / count(*) * 100, 1) as investment_adoption_rate,
        
        -- Multi-product adoption
        round(count(case when (case when total_deposit_accounts > 0 then 1 else 0 end) + 
                           (case when total_cards > 0 then 1 else 0 end) + 
                           (case when total_loans > 0 then 1 else 0 end) + 
                           (case when total_investments > 0 then 1 else 0 end) >= 2 then 1 end)::numeric / count(*) * 100, 1) as multi_product_rate,
        
        -- Cross-sell potential
        avg(cross_sell_opportunity_score) as avg_cross_sell_score,
        count(case when cross_sell_opportunity_score >= 75 then 1 end) as high_cross_sell_count,
        
        -- Digital adoption
        round(count(case when digital_adoption_level in ('DIGITAL_NATIVE', 'DIGITAL_ADOPTER') then 1 end)::numeric / count(*) * 100, 1) as digital_adoption_rate,
        
        -- Product stickiness and profitability
        avg(product_stickiness_score) as avg_stickiness_score,
        avg(account_profitability_score) as avg_profitability_score,
        sum(total_relationship_value) as segment_total_value
        
    from product_customer_analysis
    where customer_value_segment is not null
    group by customer_value_segment
),

next_best_product_analysis as (
    select
        next_best_product,
        count(*) as opportunity_count,
        avg(cross_sell_opportunity_score) as avg_opportunity_score,
        sum(total_relationship_value) as potential_customer_value,
        avg(total_relationship_value) as avg_customer_value,
        round(count(case when digital_adoption_level in ('DIGITAL_NATIVE', 'DIGITAL_ADOPTER') then 1 end)::numeric / count(*) * 100, 1) as digital_ready_rate,
        count(case when customer_value_segment = 'High Value' then 1 end) as high_value_opportunities
    from product_customer_analysis
    where next_best_product is not null
    group by next_best_product
),

channel_product_effectiveness as (
    select
        channel_preference,
        count(*) as customer_count,
        avg(product_penetration_score) as avg_penetration_score,
        avg(cross_sell_opportunity_score) as avg_cross_sell_score,
        avg(service_satisfaction_score) as avg_satisfaction_score,
        sum(total_relationship_value) as channel_total_value,
        count(case when cross_sell_opportunity_score >= 75 then 1 end) as high_cross_sell_customers
    from product_customer_analysis
    where channel_preference is not null
    group by channel_preference
)

select
    'PRODUCT_PORTFOLIO_OVERVIEW' as analytics_section,
    current_date as report_date,
    
    -- Portfolio Scale
    ppm.total_customers,
    ppm.deposit_account_customers,
    ppm.card_customers,
    ppm.loan_customers,
    ppm.investment_customers,
    
    -- Product Penetration Performance
    ppm.deposit_penetration_rate,
    ppm.card_penetration_rate,
    ppm.loan_penetration_rate,
    ppm.investment_penetration_rate,
    
    -- Multi-Product Success
    ppm.multi_product_customers,
    round(ppm.multi_product_customers::numeric / ppm.total_customers * 100, 1) as multi_product_rate,
    ppm.premium_product_customers,
    round(ppm.premium_product_customers::numeric / ppm.total_customers * 100, 1) as premium_product_rate,
    
    -- Product Performance Metrics
    round(ppm.avg_product_penetration_score::numeric, 1) as avg_product_penetration_score,
    round(ppm.avg_cross_sell_opportunity::numeric, 1) as avg_cross_sell_opportunity_score,
    round(ppm.avg_product_stickiness::numeric, 1) as avg_product_stickiness_score,
    round(ppm.avg_account_profitability::numeric, 1) as avg_account_profitability_score,
    
    -- Digital Product Adoption
    ppm.digital_native_customers,
    ppm.digital_adopter_customers,
    ppm.digital_preferred_customers,
    ppm.digital_adoption_rate,
    
    -- Cross-sell Opportunity Pipeline
    ppm.high_cross_sell_opportunity,
    ppm.medium_cross_sell_opportunity,
    round(ppm.high_cross_sell_opportunity::numeric / ppm.total_customers * 100, 1) as high_cross_sell_rate,
    
    -- Financial Performance
    ppm.total_product_portfolio_value,
    round(ppm.avg_customer_relationship_value::numeric, 0) as avg_customer_relationship_value,
    
    -- Strategic Insights
    case 
        when ppm.total_customers = 0 or ppm.total_customers is null then 'CROSS_SELL_IMPROVEMENT_NEEDED'
        when ppm.multi_product_customers::numeric / ppm.total_customers >= 0.6 then 'EXCELLENT_CROSS_SELL_PERFORMANCE'
        when ppm.multi_product_customers::numeric / ppm.total_customers >= 0.4 then 'STRONG_CROSS_SELL_PERFORMANCE'
        when ppm.multi_product_customers::numeric / ppm.total_customers >= 0.25 then 'MODERATE_CROSS_SELL_PERFORMANCE'
        else 'CROSS_SELL_IMPROVEMENT_NEEDED'
    end as cross_sell_performance_status,
    
    case 
        when ppm.total_customers = 0 or ppm.total_customers is null then 'DIGITAL_TRANSFORMATION_NEEDED'
        when ppm.digital_preferred_customers::numeric / ppm.total_customers >= 0.7 then 'DIGITAL_PRODUCT_LEADER'
        when ppm.digital_preferred_customers::numeric / ppm.total_customers >= 0.5 then 'DIGITAL_PROGRESSIVE'
        when ppm.digital_preferred_customers::numeric / ppm.total_customers >= 0.3 then 'DIGITAL_DEVELOPING'
        else 'DIGITAL_TRANSFORMATION_NEEDED'
    end as digital_product_maturity,
    
    case 
        when ppm.investment_penetration_rate >= 25 then 'STRONG_WEALTH_PRODUCTS'
        when ppm.investment_penetration_rate >= 15 then 'DEVELOPING_WEALTH_PRODUCTS'
        when ppm.investment_penetration_rate >= 5 then 'EMERGING_WEALTH_PRODUCTS'
        else 'WEALTH_PRODUCT_OPPORTUNITY'
    end as wealth_product_performance,
    
    -- Recommended Actions
    case 
        when ppm.total_customers = 0 or ppm.total_customers is null then 'OPTIMIZE_CURRENT_PORTFOLIO'
        when ppm.high_cross_sell_opportunity >= ppm.total_customers * 0.3 then 'AGGRESSIVE_CROSS_SELL_CAMPAIGN'
        when ppm.investment_penetration_rate < 15 and ppm.avg_customer_relationship_value > 50000 then 'WEALTH_PRODUCT_FOCUS'
        when ppm.digital_adoption_rate < 50 then 'DIGITAL_PRODUCT_ENHANCEMENT'
        when ppm.multi_product_customers::numeric / ppm.total_customers * 100 < 40 then 'PRODUCT_BUNDLING_STRATEGY'
        else 'OPTIMIZE_CURRENT_PORTFOLIO'
    end as recommended_product_strategy,
    
    -- Priority Product Initiatives
    case 
        when ppm.loan_penetration_rate < ppm.deposit_penetration_rate * 0.3 then 'LENDING_PRODUCT_EXPANSION'
        when ppm.card_penetration_rate < 60 then 'PAYMENT_PRODUCT_GROWTH'
        when ppm.investment_penetration_rate < 20 then 'WEALTH_MANAGEMENT_DEVELOPMENT'
        else 'PRODUCT_OPTIMIZATION'
    end as priority_product_focus,
    
    current_timestamp as last_updated

from product_portfolio_metrics ppm

union all

select
    'SEGMENT_PRODUCT_PERFORMANCE' as analytics_section,
    current_date as report_date,
    
    -- Segment Performance (separate rows for each segment)
    psa.segment_customers as total_customers,
    psa.segment_customers as deposit_account_customers,
    null as card_customers,
    null as loan_customers,
    null as investment_customers,
    
    psa.deposit_adoption_rate as deposit_penetration_rate,
    psa.card_adoption_rate as card_penetration_rate,
    psa.loan_adoption_rate as loan_penetration_rate,
    psa.investment_adoption_rate as investment_penetration_rate,
    
    null as multi_product_customers,
    psa.multi_product_rate,
    null as premium_product_customers,
    null as premium_product_rate,
    
    round(psa.avg_penetration_score::numeric, 1) as avg_product_penetration_score,
    round(psa.avg_cross_sell_score::numeric, 1) as avg_cross_sell_opportunity_score,
    round(psa.avg_stickiness_score::numeric, 1) as avg_product_stickiness_score,
    round(psa.avg_profitability_score::numeric, 1) as avg_account_profitability_score,
    
    null as digital_native_customers,
    null as digital_adopter_customers,
    null as digital_preferred_customers,
    psa.digital_adoption_rate,
    
    psa.high_cross_sell_count as high_cross_sell_opportunity,
    null as medium_cross_sell_opportunity,
    round(psa.high_cross_sell_count::numeric / psa.segment_customers * 100, 1) as high_cross_sell_rate,
    
    psa.segment_total_value as total_product_portfolio_value,
    round(psa.segment_total_value / psa.segment_customers::numeric, 0) as avg_customer_relationship_value,
    
    case 
        when psa.customer_value_segment = 'High Value' then 'EXCELLENT_CROSS_SELL_PERFORMANCE'
        when psa.customer_value_segment = 'Medium Value' then 'STRONG_CROSS_SELL_PERFORMANCE'
        when psa.customer_value_segment = 'Standard Value' then 'MODERATE_CROSS_SELL_PERFORMANCE'
        when psa.customer_value_segment is null then 'CROSS_SELL_IMPROVEMENT_NEEDED'
        else 'CROSS_SELL_IMPROVEMENT_NEEDED'
    end as cross_sell_performance_status,
    null as digital_product_maturity,
    null as wealth_product_performance,
    null as recommended_product_strategy,
    null as priority_product_focus,
    
    current_timestamp as last_updated

from product_segment_analysis psa

union all

select
    'NEXT_BEST_PRODUCT_OPPORTUNITIES' as analytics_section,
    current_date as report_date,
    
    -- Next Best Product Analysis (separate rows for each product)
    nbpa.opportunity_count as total_customers,
    nbpa.opportunity_count as deposit_account_customers,
    null as card_customers,
    null as loan_customers,
    null as investment_customers,
    
    null as deposit_penetration_rate,
    null as card_penetration_rate,
    null as loan_penetration_rate,
    null as investment_penetration_rate,
    
    null as multi_product_customers,
    null as multi_product_rate,
    null as premium_product_customers,
    null as premium_product_rate,
    
    null as avg_product_penetration_score,
    round(nbpa.avg_opportunity_score::numeric, 1) as avg_cross_sell_opportunity_score,
    null as avg_product_stickiness_score,
    null as avg_account_profitability_score,
    
    null as digital_native_customers,
    null as digital_adopter_customers,
    null as digital_preferred_customers,
    nbpa.digital_ready_rate as digital_adoption_rate,
    
    nbpa.high_value_opportunities as high_cross_sell_opportunity,
    null as medium_cross_sell_opportunity,
    null as high_cross_sell_rate,
    
    nbpa.potential_customer_value as total_product_portfolio_value,
    round(nbpa.avg_customer_value::numeric, 0) as avg_customer_relationship_value,
    
    'NEXT_BEST_PRODUCT' as cross_sell_performance_status,
    null as digital_product_maturity,
    null as wealth_product_performance,
    null as recommended_product_strategy,
    null as priority_product_focus,
    
    current_timestamp as last_updated

from next_best_product_analysis nbpa

order by analytics_section, deposit_penetration_rate desc nulls last 
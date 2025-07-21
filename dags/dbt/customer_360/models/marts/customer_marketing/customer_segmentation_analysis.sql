{{ config(
    materialized='table',
    tags=['gold', 'mart', 'marketing', 'segmentation']
) }}

/*
    Customer Segmentation Analysis
    
    Comprehensive customer segmentation and marketing effectiveness analysis:
    - Customer value segment performance and characteristics
    - Digital adoption and channel preferences by segment
    - Product penetration and cross-sell opportunities
    - Marketing campaign effectiveness and ROI
    - Retention and engagement patterns by segment
    
    Provides CMO/Marketing Director insights for customer strategy,
    targeting, and marketing optimization.
*/

with customer_segment_metrics as (
    select
        cp.customer_id,
        cp.customer_value_segment,
        cp.lifecycle_stage_name,
        cp.engagement_level,
        cp.digital_preference,
        cp.age,
        cp.relationship_years,
        
        -- Financial metrics
        cfs.total_relationship_value,
        cfs.wealth_tier,
        cfs.product_penetration_score,
        cfs.net_worth_with_bank,
        
        -- Digital and channel effectiveness
        ce.digital_adoption_score,
        ce.channel_preference,
        ce.channel_diversity_score,
        ce.service_satisfaction_score,
        
        -- Product analytics
        ppa.cross_sell_opportunity_score,
        ppa.product_stickiness_score,
        ppa.digital_adoption_level,
        ppa.marketing_responsiveness,
        ppa.next_best_product,
        
        -- Retention insights
        cra.churn_risk_score,
        cra.retention_opportunity_score,
        cra.retention_lifecycle_stage,
        cra.retention_priority,
        
        -- Risk context
        crp.overall_risk_level,
        crp.compliance_status
        
    from {{ ref('int_customer_profile') }} cp
    left join {{ ref('int_customer_financial_summary') }} cfs on cp.customer_id = cfs.customer_id
    left join {{ ref('int_channel_effectiveness') }} ce on cp.customer_id = ce.customer_id
    left join {{ ref('int_product_penetration_analysis') }} ppa on cp.customer_id = ppa.customer_id
    left join {{ ref('int_customer_retention_analytics') }} cra on cp.customer_id = cra.customer_id
    left join {{ ref('int_comprehensive_risk_profile') }} crp on cp.customer_id = crp.customer_id
),

segment_portfolio_analysis as (
    select
        customer_value_segment,
        
        -- Portfolio Size
        count(*) as segment_customer_count,
        round(count(*)::numeric / sum(count(*)) over () * 100, 1) as segment_share_percentage,
        
        -- Financial Performance
        sum(total_relationship_value) as segment_total_value,
        avg(total_relationship_value) as segment_avg_customer_value,
        round(sum(total_relationship_value) / sum(sum(total_relationship_value)) over () * 100, 1) as segment_value_share_pct,
        
        -- Demographics
        avg(age) as avg_age,
        avg(relationship_years) as avg_relationship_tenure,
        
        -- Engagement Patterns
        count(case when engagement_level = 'HIGHLY_ENGAGED' then 1 end) as highly_engaged_count,
        round(count(case when engagement_level = 'HIGHLY_ENGAGED' then 1 end)::numeric / count(*) * 100, 1) as engagement_rate_pct,
        avg(service_satisfaction_score) as avg_satisfaction_score,
        
        -- Digital Adoption
        avg(digital_adoption_score) as avg_digital_adoption,
        count(case when digital_preference in ('DIGITAL_FIRST', 'DIGITAL_PREFERRED') then 1 end) as digital_preferred_customers,
        round(count(case when digital_preference in ('DIGITAL_FIRST', 'DIGITAL_PREFERRED') then 1 end)::numeric / count(*) * 100, 1) as digital_adoption_rate_pct,
        
        -- Product Performance
        avg(product_penetration_score) as avg_product_penetration,
        avg(cross_sell_opportunity_score) as avg_cross_sell_opportunity,
        count(case when cross_sell_opportunity_score >= 75 then 1 end) as high_cross_sell_customers,
        avg(product_stickiness_score) as avg_product_stickiness,
        
        -- Marketing Responsiveness
        count(case when marketing_responsiveness = 'HIGHLY_RESPONSIVE' then 1 end) as highly_responsive_customers,
        count(case when marketing_responsiveness = 'MODERATELY_RESPONSIVE' then 1 end) as moderately_responsive_customers,
        count(case when marketing_responsiveness = 'UNRESPONSIVE' then 1 end) as unresponsive_customers,
        round(count(case when marketing_responsiveness in ('HIGHLY_RESPONSIVE', 'MODERATELY_RESPONSIVE') then 1 end)::numeric / count(*) * 100, 1) as marketing_responsiveness_rate,
        
        -- Retention Metrics
        avg(churn_risk_score) as avg_churn_risk,
        avg(retention_opportunity_score) as avg_retention_opportunity,
        count(case when retention_priority in ('CRITICAL', 'WIN_BACK') then 1 end) as critical_retention_cases,
        count(case when churn_risk_score >= 70 then 1 end) as high_churn_risk_customers,
        
        -- Risk Profile
        count(case when overall_risk_level in ('HIGH_RISK', 'CRITICAL_RISK') then 1 end) as high_risk_customers,
        count(case when compliance_status != 'COMPLIANT' then 1 end) as non_compliant_customers
        
    from customer_segment_metrics
    where customer_value_segment is not null
    group by customer_value_segment
),

lifecycle_stage_analysis as (
    select
        lifecycle_stage_name,
        count(*) as lifecycle_customer_count,
        avg(total_relationship_value) as avg_customer_value,
        avg(product_penetration_score) as avg_product_penetration,
        avg(digital_adoption_score) as avg_digital_adoption,
        avg(churn_risk_score) as avg_churn_risk,
        count(case when cross_sell_opportunity_score >= 75 then 1 end) as cross_sell_opportunities,
        round(count(case when engagement_level = 'HIGHLY_ENGAGED' then 1 end)::numeric / count(*) * 100, 1) as engagement_rate_pct
    from customer_segment_metrics
    where lifecycle_stage_name is not null
    group by lifecycle_stage_name
),

digital_preference_segments as (
    select
        digital_preference,
        channel_preference,
        count(*) as customer_count,
        avg(digital_adoption_score) as avg_digital_score,
        avg(channel_diversity_score) as avg_channel_diversity,
        avg(service_satisfaction_score) as avg_satisfaction,
        avg(total_relationship_value) as avg_customer_value,
        round(count(case when marketing_responsiveness in ('HIGHLY_RESPONSIVE', 'MODERATELY_RESPONSIVE') then 1 end)::numeric / count(*) * 100, 1) as marketing_response_rate
    from customer_segment_metrics
    where digital_preference is not null and channel_preference is not null
    group by digital_preference, channel_preference
),

segment_profitability_analysis as (
    select
        spa.customer_value_segment,
        spa.segment_customer_count,
        spa.segment_total_value,
        spa.segment_avg_customer_value,
        
        -- ROI Calculations (using engagement and satisfaction as proxies)
        spa.avg_satisfaction_score * spa.segment_total_value / 1000000 as customer_satisfaction_value_index,
        spa.avg_cross_sell_opportunity * spa.segment_customer_count / 100 as growth_potential_index,
        spa.avg_retention_opportunity * spa.segment_total_value / 10000000 as retention_value_index,
        
        -- Marketing Efficiency
        spa.marketing_responsiveness_rate / 100 * spa.segment_customer_count as responsive_customer_base,
        spa.digital_adoption_rate_pct / 100 * spa.segment_customer_count as digital_ready_customers,
        
        -- Risk Adjusted Value
        (100 - coalesce(spa.avg_churn_risk, 0)) / 100 * spa.segment_total_value as retention_adjusted_value
        
    from segment_portfolio_analysis spa
)

select
    'SEGMENT_OVERVIEW' as analysis_section,
    current_date as report_date,
    
    -- Segment Portfolio Performance
    spa.customer_value_segment,
    spa.segment_customer_count,
    spa.segment_share_percentage,
    spa.segment_total_value,
    round(spa.segment_avg_customer_value::numeric, 0) as avg_customer_value,
    spa.segment_value_share_pct,
    
    -- Customer Characteristics
    round(spa.avg_age::numeric, 1) as avg_customer_age,
    round(spa.avg_relationship_tenure::numeric, 1) as avg_relationship_years,
    spa.highly_engaged_count,
    spa.engagement_rate_pct,
    round(spa.avg_satisfaction_score::numeric, 2) as avg_satisfaction_score,
    
    -- Digital & Channel Performance
    round(spa.avg_digital_adoption::numeric, 1) as avg_digital_adoption_score,
    spa.digital_preferred_customers,
    spa.digital_adoption_rate_pct,
    round(spa.avg_product_penetration::numeric, 1) as avg_product_penetration_score,
    
    -- Marketing & Growth Opportunities
    spa.highly_responsive_customers,
    spa.moderately_responsive_customers,
    spa.marketing_responsiveness_rate as marketing_response_rate_pct,
    spa.high_cross_sell_customers,
    round(spa.avg_cross_sell_opportunity::numeric, 1) as avg_cross_sell_score,
    round(spa.avg_product_stickiness::numeric, 1) as avg_product_stickiness_score,
    
    -- Retention & Risk
    round(spa.avg_churn_risk::numeric, 1) as avg_churn_risk_score,
    spa.high_churn_risk_customers,
    spa.critical_retention_cases,
    round(spa.avg_retention_opportunity::numeric, 1) as avg_retention_opportunity_score,
    spa.high_risk_customers,
    
    -- Strategic Insights
    case 
        when spa.segment_value_share_pct >= 40 then 'PRIMARY_VALUE_DRIVER'
        when spa.segment_value_share_pct >= 20 then 'MAJOR_CONTRIBUTOR'
        when spa.segment_value_share_pct >= 10 then 'SIGNIFICANT_SEGMENT'
        else 'NICHE_SEGMENT'
    end as segment_strategic_importance,
    
    case 
        when spa.marketing_responsiveness_rate >= 70 then 'HIGHLY_MARKETABLE'
        when spa.marketing_responsiveness_rate >= 50 then 'MODERATELY_MARKETABLE'
        when spa.marketing_responsiveness_rate >= 30 then 'SELECTIVE_MARKETING'
        else 'MARKETING_CHALLENGE'
    end as marketing_effectiveness,
    
    case 
        when spa.digital_adoption_rate_pct >= 70 then 'DIGITAL_NATIVE'
        when spa.digital_adoption_rate_pct >= 50 then 'DIGITAL_ADOPTER'
        when spa.digital_adoption_rate_pct >= 30 then 'DIGITAL_OPPORTUNITY'
        else 'TRADITIONAL_PREFERRED'
    end as digital_strategy_classification,
    
    case 
        when spa.avg_churn_risk <= 30 and spa.avg_retention_opportunity >= 70 then 'LOYALTY_CHAMPIONS'
        when spa.avg_churn_risk <= 50 and spa.avg_retention_opportunity >= 50 then 'STABLE_RELATIONSHIPS'
        when spa.avg_churn_risk >= 70 then 'RETENTION_CRITICAL'
        else 'STANDARD_RETENTION'
    end as retention_strategy_classification,
    
    -- Recommended Actions
    case 
        when spa.high_cross_sell_customers >= spa.segment_customer_count * 0.5 then 'AGGRESSIVE_CROSS_SELL_CAMPAIGN'
        when spa.digital_adoption_rate_pct < 40 and spa.customer_value_segment = 'High Value' then 'DIGITAL_TRANSFORMATION_FOCUS'
        when spa.marketing_responsiveness_rate < 30 then 'MESSAGING_STRATEGY_REVIEW'
        when spa.critical_retention_cases >= spa.segment_customer_count * 0.1 then 'RETENTION_PROGRAM_PRIORITY'
        else 'MAINTAIN_CURRENT_STRATEGY'
    end as recommended_marketing_action,
    
    -- Performance Indicators
    round(spfa.customer_satisfaction_value_index::numeric, 2) as satisfaction_value_index,
    round(spfa.growth_potential_index::numeric, 0) as growth_potential_index,
    round(spfa.retention_value_index::numeric, 2) as retention_value_index,
    round(spfa.responsive_customer_base::numeric, 0) as marketing_responsive_base,
    round(spfa.retention_adjusted_value::numeric, 0) as risk_adjusted_segment_value,
    
    current_timestamp as last_updated

from segment_portfolio_analysis spa
left join segment_profitability_analysis spfa on spa.customer_value_segment = spfa.customer_value_segment

union all

select
    'LIFECYCLE_ANALYSIS' as analysis_section,
    current_date as report_date,
    
    -- Lifecycle Stage Metrics (separate rows for each stage)
    lsa.lifecycle_stage_name as customer_value_segment,
    lsa.lifecycle_customer_count as segment_customer_count,
    null as segment_share_percentage,
    null as segment_total_value,
    round(lsa.avg_customer_value::numeric, 0) as avg_customer_value,
    null as segment_value_share_pct,
    
    null as avg_customer_age,
    null as avg_relationship_years,
    null as highly_engaged_count,
    lsa.engagement_rate_pct,
    null as avg_satisfaction_score,
    
    round(lsa.avg_digital_adoption::numeric, 1) as avg_digital_adoption_score,
    null as digital_preferred_customers,
    null as digital_adoption_rate_pct,
    round(lsa.avg_product_penetration::numeric, 1) as avg_product_penetration_score,
    
    null as highly_responsive_customers,
    null as moderately_responsive_customers,
    null as marketing_response_rate_pct,
    lsa.cross_sell_opportunities as high_cross_sell_customers,
    null as avg_cross_sell_score,
    null as avg_product_stickiness_score,
    
    round(lsa.avg_churn_risk::numeric, 1) as avg_churn_risk_score,
    null as high_churn_risk_customers,
    null as critical_retention_cases,
    null as avg_retention_opportunity_score,
    null as high_risk_customers,
    
    'LIFECYCLE_STAGE' as segment_strategic_importance,
    null as marketing_effectiveness,
    null as digital_strategy_classification,
    null as retention_strategy_classification,
    null as recommended_marketing_action,
    
    null as satisfaction_value_index,
    null as growth_potential_index,
    null as retention_value_index,
    null as marketing_responsive_base,
    null as risk_adjusted_segment_value,
    
    current_timestamp as last_updated

from lifecycle_stage_analysis lsa

order by analysis_section, segment_share_percentage desc nulls last, avg_customer_value desc nulls last 
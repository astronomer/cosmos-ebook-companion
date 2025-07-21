{{ config(
    materialized='table',
    tags=['gold', 'mart', 'executive', 'dashboard']
) }}

/*
    Executive Customer Dashboard
    
    Ultimate C-suite view combining insights from all intermediate models:
    - Customer profile and segmentation insights
    - Financial relationship health and value
    - Risk and compliance status
    - Engagement and retention metrics
    - Channel effectiveness and digital adoption
    - Account performance and profitability
    
    Provides executive-level KPIs, trends, and strategic insights for
    customer strategy, risk management, and business growth.
*/

with customer_health_metrics as (
    select
        cp.customer_id,
        cp.full_name,
        cp.age,
        cp.relationship_years,
        cp.customer_value_segment,
        cp.lifecycle_stage_name,
        cp.engagement_level,
        cp.at_risk_customer,
        cp.digital_preference,
        
        -- Financial Health
        cfs.total_relationship_value,
        cfs.net_worth_with_bank,
        cfs.product_penetration_score,
        cfs.wealth_tier,
        cfs.financial_risk_level,
        
        -- Risk Assessment
        crp.overall_risk_level,
        crp.compliance_status,
        crp.composite_risk_score,
        crp.requires_enhanced_monitoring,
        crp.requires_immediate_review,
        
        -- Digital & Channel Engagement
        ce.digital_adoption_score,
        ce.channel_diversity_score,
        ce.channel_preference,
        ce.service_satisfaction_score,
        
        -- Retention Analytics
        cra.churn_risk_score,
        cra.retention_opportunity_score,
        cra.retention_lifecycle_stage,
        cra.retention_priority,
        
        -- Product Analytics
        ppa.cross_sell_opportunity_score,
        ppa.product_stickiness_score,
        ppa.digital_adoption_level,
        ppa.next_best_product
        
    from {{ ref('int_customer_profile') }} cp
    left join {{ ref('int_customer_financial_summary') }} cfs on cp.customer_id = cfs.customer_id
    left join {{ ref('int_comprehensive_risk_profile') }} crp on cp.customer_id = crp.customer_id
    left join {{ ref('int_channel_effectiveness') }} ce on cp.customer_id = ce.customer_id
    left join {{ ref('int_customer_retention_analytics') }} cra on cp.customer_id = cra.customer_id
    left join {{ ref('int_product_penetration_analysis') }} ppa on cp.customer_id = ppa.customer_id
),

executive_summary as (
    select
        -- Customer Portfolio Health
        count(*) as total_customers,
        count(case when customer_value_segment = 'High Value' then 1 end) as high_value_customers,
        count(case when wealth_tier in ('PRIVATE_BANKING', 'WEALTH_MANAGEMENT') then 1 end) as wealth_management_customers,
        count(case when engagement_level = 'HIGHLY_ENGAGED' then 1 end) as highly_engaged_customers,
        count(case when at_risk_customer then 1 end) as at_risk_customers,
        
        -- Financial Performance
        sum(total_relationship_value) as total_customer_assets,
        avg(total_relationship_value) as avg_relationship_value,
        sum(case when wealth_tier = 'PRIVATE_BANKING' then total_relationship_value else 0 end) as private_banking_assets,
        avg(product_penetration_score) as avg_product_penetration,
        
        -- Risk Portfolio
        count(case when overall_risk_level in ('HIGH_RISK', 'CRITICAL_RISK') then 1 end) as high_risk_customers,
        count(case when compliance_status != 'COMPLIANT' then 1 end) as non_compliant_customers,
        count(case when requires_immediate_review then 1 end) as customers_needing_review,
        avg(composite_risk_score) as avg_risk_score,
        
        -- Digital Transformation
        count(case when digital_preference in ('DIGITAL_FIRST', 'DIGITAL_PREFERRED') then 1 end) as digital_preferred_customers,
        avg(digital_adoption_score) as avg_digital_adoption,
        count(case when channel_preference = 'OMNI_CHANNEL' then 1 end) as omni_channel_customers,
        
        -- Retention & Growth
        count(case when churn_risk_score >= 70 then 1 end) as high_churn_risk_customers,
        count(case when retention_priority = 'CRITICAL' then 1 end) as critical_retention_cases,
        count(case when cross_sell_opportunity_score >= 75 then 1 end) as high_cross_sell_opportunities,
        avg(retention_opportunity_score) as avg_retention_opportunity,
        
        -- Service Excellence
        avg(service_satisfaction_score) as avg_service_satisfaction,
        count(case when service_satisfaction_score >= 4 then 1 end) as highly_satisfied_customers,
        count(case when service_satisfaction_score <= 2 then 1 end) as dissatisfied_customers
        
    from customer_health_metrics
),

segment_performance as (
    select
        customer_value_segment,
        count(*) as customer_count,
        round(count(*)::numeric / sum(count(*)) over () * 100, 1) as segment_percentage,
        sum(total_relationship_value) as segment_assets,
        avg(total_relationship_value) as avg_relationship_value,
        avg(product_penetration_score) as avg_product_penetration,
        avg(digital_adoption_score) as avg_digital_adoption,
        avg(churn_risk_score) as avg_churn_risk,
        count(case when at_risk_customer then 1 end) as at_risk_count,
        count(case when cross_sell_opportunity_score >= 75 then 1 end) as cross_sell_opportunities
    from customer_health_metrics
    where customer_value_segment is not null
    group by customer_value_segment
),

risk_heatmap as (
    select
        overall_risk_level,
        financial_risk_level,
        count(*) as customer_count,
        sum(total_relationship_value) as assets_at_risk,
        avg(composite_risk_score) as avg_risk_score,
        count(case when requires_immediate_review then 1 end) as immediate_action_required
    from customer_health_metrics
    where overall_risk_level is not null and financial_risk_level is not null
    group by overall_risk_level, financial_risk_level
),

retention_insights as (
    select
        retention_lifecycle_stage,
        count(*) as customer_count,
        sum(total_relationship_value) as revenue_at_risk,
        avg(churn_risk_score) as avg_churn_risk,
        avg(retention_opportunity_score) as avg_retention_opportunity,
        count(case when retention_priority in ('CRITICAL', 'WIN_BACK') then 1 end) as priority_actions_needed
    from customer_health_metrics
    where retention_lifecycle_stage is not null
    group by retention_lifecycle_stage
),

digital_transformation_metrics as (
    select
        digital_preference,
        channel_preference,
        count(*) as customer_count,
        avg(digital_adoption_score) as avg_digital_adoption,
        avg(channel_diversity_score) as avg_channel_diversity,
        avg(service_satisfaction_score) as avg_satisfaction,
        sum(total_relationship_value) as segment_value
    from customer_health_metrics
    where digital_preference is not null and channel_preference is not null
    group by digital_preference, channel_preference
)

select
    'EXECUTIVE_SUMMARY' as dashboard_section,
    current_date as report_date,
    
    -- Portfolio Overview
    es.total_customers,
    es.high_value_customers,
    round(es.high_value_customers::numeric / es.total_customers * 100, 1) as high_value_percentage,
    es.wealth_management_customers,
    es.highly_engaged_customers,
    round(es.highly_engaged_customers::numeric / es.total_customers * 100, 1) as engagement_rate,
    
    -- Financial Health
    es.total_customer_assets,
    es.avg_relationship_value,
    es.private_banking_assets,
    round(es.private_banking_assets / es.total_customer_assets * 100, 1) as private_banking_share,
    round(es.avg_product_penetration, 1) as avg_product_penetration,
    
    -- Risk Management
    es.high_risk_customers,
    round(es.high_risk_customers::numeric / es.total_customers * 100, 1) as high_risk_percentage,
    es.non_compliant_customers,
    es.customers_needing_review,
    round(es.avg_risk_score, 0) as avg_risk_score,
    
    -- Digital Excellence
    es.digital_preferred_customers,
    round(es.digital_preferred_customers::numeric / es.total_customers * 100, 1) as digital_adoption_rate,
    round(es.avg_digital_adoption, 1) as avg_digital_score,
    es.omni_channel_customers,
    
    -- Retention & Growth
    es.high_churn_risk_customers,
    round(es.high_churn_risk_customers::numeric / es.total_customers * 100, 1) as churn_risk_rate,
    es.critical_retention_cases,
    es.high_cross_sell_opportunities,
    round(es.avg_retention_opportunity, 1) as avg_retention_opportunity,
    
    -- Service Excellence
    round(es.avg_service_satisfaction, 2) as avg_service_satisfaction,
    es.highly_satisfied_customers,
    round(es.highly_satisfied_customers::numeric / es.total_customers * 100, 1) as satisfaction_rate,
    es.dissatisfied_customers,
    
    -- Strategic Insights
    case 
        when es.total_customers = 0 or es.total_customers is null or es.high_value_customers is null then 'GROWTH_OPPORTUNITY'
        when es.high_value_customers::numeric / es.total_customers >= 0.2 then 'STRONG_VALUE_MIX'
        when es.high_value_customers::numeric / es.total_customers >= 0.1 then 'BALANCED_PORTFOLIO' 
        else 'GROWTH_OPPORTUNITY'
    end as portfolio_health,
    
    case 
        when es.avg_digital_adoption is null then 'DIGITAL_OPPORTUNITY'
        when es.avg_digital_adoption >= 75 then 'DIGITAL_LEADER'
        when es.avg_digital_adoption >= 50 then 'DIGITAL_PROGRESSIVE'
        else 'DIGITAL_OPPORTUNITY'
    end as digital_maturity,
    
    case 
        when es.high_risk_customers::numeric / es.total_customers <= 0.05 then 'LOW_RISK_PORTFOLIO'
        when es.high_risk_customers::numeric / es.total_customers <= 0.15 then 'MANAGED_RISK'
        else 'HIGH_RISK_ATTENTION_NEEDED'
    end as risk_profile_status,
    
    -- Priority Actions
    greatest(es.customers_needing_review, 0) as immediate_actions_required,
    greatest(es.critical_retention_cases, 0) as retention_actions_required,
    greatest(es.high_cross_sell_opportunities, 0) as growth_opportunities_available,
    
    current_timestamp as last_updated

from executive_summary es

union all

select
    'SEGMENT_PERFORMANCE' as dashboard_section,
    current_date as report_date,
    
    -- Segment Metrics (will create separate rows for each segment)
    sp.customer_count as total_customers,
    null as high_value_customers,
    sp.segment_percentage as high_value_percentage,
    null as wealth_management_customers,
    null as highly_engaged_customers,
    null as engagement_rate,
    
    sp.segment_assets as total_customer_assets,
    sp.avg_relationship_value,
    null as private_banking_assets,
    null as private_banking_share,
    sp.avg_product_penetration,
    
    null as high_risk_customers,
    null as high_risk_percentage,
    null as non_compliant_customers,
    null as customers_needing_review,
    null as avg_risk_score,
    
    null as digital_preferred_customers,
    null as digital_adoption_rate,
    sp.avg_digital_adoption as avg_digital_score,
    null as omni_channel_customers,
    
    null as high_churn_risk_customers,
    sp.avg_churn_risk as churn_risk_rate,
    null as critical_retention_cases,
    sp.cross_sell_opportunities as high_cross_sell_opportunities,
    null as avg_retention_opportunity,
    
    null as avg_service_satisfaction,
    null as highly_satisfied_customers,
    null as satisfaction_rate,
    null as dissatisfied_customers,
    
    case 
        when sp.customer_value_segment = 'High Value' then 'STRONG_VALUE_MIX'
        when sp.customer_value_segment = 'Medium Value' then 'BALANCED_PORTFOLIO'
        when sp.customer_value_segment = 'Standard Value' then 'GROWTH_OPPORTUNITY'
        else 'GROWTH_OPPORTUNITY'
    end as portfolio_health,
    null as digital_maturity,
    null as risk_profile_status,
    
    null as immediate_actions_required,
    sp.at_risk_count as retention_actions_required,
    sp.cross_sell_opportunities as growth_opportunities_available,
    
    current_timestamp as last_updated

from segment_performance sp
where sp.customer_value_segment is not null

order by dashboard_section, high_value_percentage desc nulls last 
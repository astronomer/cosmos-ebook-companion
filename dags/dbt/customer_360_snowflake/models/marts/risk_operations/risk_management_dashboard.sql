{{ config(
    materialized='table',
    tags=['gold', 'mart', 'risk', 'compliance']
) }}

/*
    Risk Management Dashboard
    
    Comprehensive risk monitoring and compliance dashboard combining:
    - Customer risk profiles and assessments
    - Fraud detection and alert management
    - Regulatory compliance status and monitoring
    - Portfolio risk concentration analysis
    - Operational risk indicators
    
    Provides CRO/Chief Compliance Officer level insights for risk governance
    and regulatory management.
*/

with customer_risk_portfolio as (
    select
        crp.customer_id,
        crp.composite_risk_rating as overall_risk_level,
        crp.compliance_status,
        crp.composite_risk_score,
        crp.current_credit_risk_score,
        crp.current_fraud_risk_score,
        crp.total_fraud_alerts,
        case when crp.overall_risk_assessment = 'REQUIRES_MONITORING' then true else false end as requires_enhanced_monitoring,
        case when crp.overall_risk_assessment = 'REQUIRES_IMMEDIATE_REVIEW' then true else false end as requires_immediate_review,
        case 
            when crp.credit_score_volatility > 50 then 'VOLATILE'
            when crp.composite_risk_score > 700 then 'DETERIORATING'
            when crp.composite_risk_score > 600 then 'UNSTABLE'
            else 'STABLE'
        end as risk_trend,
        
        -- Customer context
        cp.customer_value_segment,
        cp.relationship_years,
        cp.engagement_level,
        cp.at_risk_customer,
        
        -- Financial context
        cfs.total_relationship_value,
        cfs.customer_value_segment as wealth_tier,
        cfs.customer_risk_profile as financial_risk_level,
        cfs.net_worth_with_bank,
        
        -- Retention context
        cra.churn_risk_score,
        cra.retention_priority,
        cra.retention_lifecycle_stage
        
    from {{ ref('int_comprehensive_risk_profile') }} crp
    left join {{ ref('int_customer_profile') }} cp on crp.customer_id = cp.customer_id
    left join {{ ref('int_customer_financial_summary') }} cfs on crp.customer_id = cfs.customer_id
    left join {{ ref('int_customer_retention_analytics') }} cra on crp.customer_id = cra.customer_id
),

risk_portfolio_summary as (
    select
        -- Portfolio Size
        count(*) as total_customers,
        count(case when total_relationship_value > 0 then 1 end) as active_customers,
        sum(total_relationship_value) as total_portfolio_value,
        
        -- Risk Level Distribution
        count(case when overall_risk_level = 'HIGH_RISK' then 1 end) as critical_risk_customers,
        count(case when overall_risk_level = 'HIGH_RISK' then 1 end) as high_risk_customers,
        count(case when overall_risk_level = 'MEDIUM_RISK' then 1 end) as elevated_risk_customers,
        count(case when overall_risk_level = 'MEDIUM_RISK' then 1 end) as moderate_risk_customers,
        count(case when overall_risk_level = 'LOW_RISK' then 1 end) as low_risk_customers,
        
        -- Assets at Risk
        sum(case when overall_risk_level = 'HIGH_RISK' then total_relationship_value else 0 end) as critical_risk_assets,
        sum(case when overall_risk_level = 'HIGH_RISK' then total_relationship_value else 0 end) as high_risk_assets,
        sum(case when overall_risk_level = 'HIGH_RISK' then total_relationship_value else 0 end) as high_risk_portfolio_value,
        
        -- Compliance Status
        count(case when compliance_status = 'MAJOR_CONCERNS' then 1 end) as non_compliant_customers,
        count(case when compliance_status = 'MINOR_CONCERNS' then 1 end) as compliance_review_required,
        count(case when compliance_status = 'MINOR_CONCERNS' then 1 end) as pending_verification,
        count(case when compliance_status = 'COMPLIANT' then 1 end) as compliant_customers,
        
        -- Action Required
        count(case when requires_immediate_review then 1 end) as immediate_review_required,
        count(case when requires_enhanced_monitoring then 1 end) as enhanced_monitoring_required,
        
        -- Fraud Indicators
        sum(total_fraud_alerts) as total_fraud_alerts,
        count(case when total_fraud_alerts > 0 then 1 end) as customers_with_fraud_alerts,
        count(case when total_fraud_alerts >= 3 then 1 end) as high_fraud_alert_customers,
        
        -- Risk Trend Analysis
        count(case when risk_trend = 'VOLATILE' then 1 end) as volatile_risk_customers,
        count(case when risk_trend = 'DETERIORATING' then 1 end) as deteriorating_risk_customers,
        count(case when risk_trend = 'UNSTABLE' then 1 end) as unstable_risk_customers,
        count(case when risk_trend = 'STABLE' then 1 end) as stable_risk_customers,
        
        -- Risk Scores
        avg(composite_risk_score) as avg_composite_risk_score,
        avg(current_credit_risk_score) as avg_credit_risk_score,
        avg(current_fraud_risk_score) as avg_fraud_risk_score,
        
        -- High Value Customer Risk
        count(case when customer_value_segment = 'High Value' and overall_risk_level = 'HIGH_RISK' then 1 end) as high_value_high_risk,
        sum(case when customer_value_segment = 'High Value' and overall_risk_level = 'HIGH_RISK' then total_relationship_value else 0 end) as high_value_high_risk_assets
        
    from customer_risk_portfolio
),

wealth_tier_risk_analysis as (
    select
        wealth_tier,
        count(*) as customer_count,
        sum(total_relationship_value) as tier_assets,
        count(case when overall_risk_level = 'HIGH_RISK' then 1 end) as high_risk_count,
        sum(case when overall_risk_level = 'HIGH_RISK' then total_relationship_value else 0 end) as high_risk_assets,
        avg(composite_risk_score) as avg_risk_score,
        count(case when requires_immediate_review then 1 end) as immediate_actions,
        count(case when compliance_status != 'COMPLIANT' then 1 end) as compliance_issues
    from customer_risk_portfolio
    where wealth_tier is not null
    group by wealth_tier
),

operational_risk_metrics as (
    select
        rps.immediate_review_required + rps.enhanced_monitoring_required as total_risk_actions_required,
        rps.non_compliant_customers + rps.compliance_review_required as total_compliance_actions,
        rps.high_fraud_alert_customers as fraud_investigation_required,
        
        -- Risk concentration
        rps.high_risk_portfolio_value / nullif(rps.total_portfolio_value, 0) * 100 as high_risk_concentration_pct,
        rps.critical_risk_customers / nullif(rps.total_customers, 0) * 100 as critical_risk_customer_pct,
        rps.non_compliant_customers / nullif(rps.total_customers, 0) * 100 as non_compliance_rate,
        
        -- Risk velocity
        rps.deteriorating_risk_customers / nullif(rps.total_customers, 0) * 100 as deteriorating_risk_rate,
        rps.volatile_risk_customers / nullif(rps.total_customers, 0) * 100 as volatile_risk_rate,
        
        -- Fraud metrics
        rps.total_fraud_alerts / nullif(rps.total_customers, 0) as fraud_alerts_per_customer,
        rps.customers_with_fraud_alerts / nullif(rps.total_customers, 0) * 100 as customers_with_fraud_pct,
        
        -- High value risk exposure
        rps.high_value_high_risk_assets / nullif(rps.total_portfolio_value, 0) * 100 as high_value_risk_exposure_pct
        
    from risk_portfolio_summary rps
)

select
    'RISK_OVERVIEW' as dashboard_section,
    current_date as report_date,
    
    -- Portfolio Risk Profile
    rps.total_customers,
    rps.total_portfolio_value,
    rps.critical_risk_customers,
    rps.high_risk_customers,
    round(orm.critical_risk_customer_pct::numeric, 1) as critical_risk_customer_percentage,
    round(orm.high_risk_concentration_pct::numeric, 1) as high_risk_asset_concentration_pct,
    
    -- Risk Assets
    rps.critical_risk_assets,
    rps.high_risk_assets,
    rps.high_risk_portfolio_value,
    rps.high_value_high_risk_assets as high_value_customers_at_risk_assets,
    
    -- Compliance Status
    rps.non_compliant_customers,
    rps.compliance_review_required,
    rps.pending_verification,
    rps.compliant_customers,
    round(orm.non_compliance_rate::numeric, 1) as non_compliance_rate_pct,
    
    -- Immediate Actions Required
    rps.immediate_review_required,
    rps.enhanced_monitoring_required,
    orm.total_risk_actions_required,
    orm.total_compliance_actions,
    
    -- Fraud Risk
    rps.total_fraud_alerts,
    rps.customers_with_fraud_alerts,
    rps.high_fraud_alert_customers,
    round(orm.fraud_alerts_per_customer::numeric, 2) as avg_fraud_alerts_per_customer,
    round(orm.customers_with_fraud_pct::numeric, 1) as customers_with_fraud_percentage,
    
    -- Risk Dynamics
    rps.deteriorating_risk_customers,
    rps.volatile_risk_customers,
    round(orm.deteriorating_risk_rate::numeric, 1) as deteriorating_risk_rate_pct,
    round(orm.volatile_risk_rate::numeric, 1) as volatile_risk_rate_pct,
    
    -- Risk Scores
    round(rps.avg_composite_risk_score::numeric, 0) as avg_composite_risk_score,
    round(rps.avg_credit_risk_score::numeric, 0) as avg_credit_risk_score,
    round(rps.avg_fraud_risk_score::numeric, 0) as avg_fraud_risk_score,
    
    -- Risk Assessment
    case 
        when orm.critical_risk_customer_pct > 10 then 'CRITICAL_RISK_PORTFOLIO'
        when orm.high_risk_concentration_pct > 25 then 'HIGH_RISK_CONCENTRATION'
        when orm.non_compliance_rate > 5 then 'COMPLIANCE_ISSUES'
        when orm.deteriorating_risk_rate > 15 then 'RISK_DETERIORATION'
        else 'MANAGED_RISK_PROFILE'
    end as portfolio_risk_status,
    
    case 
        when orm.non_compliance_rate > 10 then 'COMPLIANCE_CRISIS'
        when orm.non_compliance_rate > 5 then 'COMPLIANCE_ATTENTION_NEEDED'
        when orm.non_compliance_rate > 2 then 'COMPLIANCE_MONITORING_REQUIRED'
        else 'COMPLIANCE_HEALTHY'
    end as compliance_health_status,
    
    case 
        when orm.customers_with_fraud_pct > 20 then 'HIGH_FRAUD_ENVIRONMENT'
        when orm.customers_with_fraud_pct > 10 then 'ELEVATED_FRAUD_RISK'
        when orm.customers_with_fraud_pct > 5 then 'MODERATE_FRAUD_ACTIVITY'
        else 'LOW_FRAUD_ENVIRONMENT'
    end as fraud_risk_environment,
    
    -- Priority Actions
    case 
        when orm.total_risk_actions_required > 100 then 'IMMEDIATE_RISK_COMMITTEE_MEETING'
        when orm.total_compliance_actions > 50 then 'COMPLIANCE_REMEDIATION_PLAN'
        when rps.high_fraud_alert_customers > 20 then 'FRAUD_INVESTIGATION_SURGE'
        when orm.high_value_risk_exposure_pct > 15 then 'HIGH_VALUE_CUSTOMER_REVIEW'
        else 'STANDARD_RISK_MONITORING'
    end as recommended_action,
    
    -- Regulatory Alerts
    case 
        when orm.non_compliance_rate > 5 or orm.total_compliance_actions > 25 then 'REGULATORY_REPORTING_REQUIRED'
        else 'NO_REGULATORY_ALERTS'
    end as regulatory_status,
    
    current_timestamp as last_updated

from risk_portfolio_summary rps
cross join operational_risk_metrics orm

union all

select
    'WEALTH_TIER_RISK_BREAKDOWN' as dashboard_section,
    current_date as report_date,
    
    -- Wealth Tier Metrics (will create separate rows for each tier)
    wtra.customer_count as total_customers,
    wtra.tier_assets as total_portfolio_value,
    wtra.high_risk_count as critical_risk_customers,
    null as high_risk_customers,
    round(wtra.high_risk_count::numeric / wtra.customer_count * 100, 1) as critical_risk_customer_percentage,
    round(wtra.high_risk_assets / nullif(wtra.tier_assets, 0) * 100, 1) as high_risk_asset_concentration_pct,
    
    null as critical_risk_assets,
    wtra.high_risk_assets,
    null as high_risk_portfolio_value,
    null as high_value_customers_at_risk_assets,
    
    wtra.compliance_issues as non_compliant_customers,
    null as compliance_review_required,
    null as pending_verification,
    null as compliant_customers,
    round(wtra.compliance_issues::numeric / wtra.customer_count * 100, 1) as non_compliance_rate_pct,
    
    wtra.immediate_actions as immediate_review_required,
    null as enhanced_monitoring_required,
    null as total_risk_actions_required,
    null as total_compliance_actions,
    
    null as total_fraud_alerts,
    null as customers_with_fraud_alerts,
    null as high_fraud_alert_customers,
    null as avg_fraud_alerts_per_customer,
    null as customers_with_fraud_percentage,
    
    null as deteriorating_risk_customers,
    null as volatile_risk_customers,
    null as deteriorating_risk_rate_pct,
    null as volatile_risk_rate_pct,
    
    round(wtra.avg_risk_score::numeric, 0) as avg_composite_risk_score,
    null as avg_credit_risk_score,
    null as avg_fraud_risk_score,
    
    case 
        when wtra.customer_count = 0 or wtra.customer_count is null then 'MANAGED_RISK_PROFILE'
        when wtra.high_risk_count::numeric / wtra.customer_count > 0.1 then 'HIGH_RISK_CONCENTRATION'
        when wtra.compliance_issues::numeric / wtra.customer_count > 0.05 then 'COMPLIANCE_ISSUES'
        when wtra.avg_risk_score > 70 then 'RISK_DETERIORATION'
        else 'MANAGED_RISK_PROFILE'
    end as portfolio_risk_status,
    null as compliance_health_status,
    null as fraud_risk_environment,
    null as recommended_action,
    null as regulatory_status,
    
    current_timestamp as last_updated

from wealth_tier_risk_analysis wtra

order by dashboard_section, critical_risk_customer_percentage desc nulls last 
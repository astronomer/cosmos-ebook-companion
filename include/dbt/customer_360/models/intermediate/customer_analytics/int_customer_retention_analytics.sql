{{ config(
    materialized='view',
    tags=['silver', 'intermediate', 'customer_analytics', 'retention']
) }}

/*
    Customer Retention Analytics
    
    Advanced retention modeling combining:
    - Retention events (stg_retention_events)
    - Customer interactions (stg_customer_interactions)
    - Digital activity (stg_digital_activity)
    - Customer profile insights (int_customer_profile)
    - Risk assessment (int_comprehensive_risk_profile)
    
    Identifies churn risk and retention opportunities with predictive scoring.
*/

with retention_events_summary as (
    select
        customer_id,
        count(*) as total_retention_events,
        count(case when event_type = 'CHURN_RISK_IDENTIFIED' then 1 end) as churn_risk_events,
        count(case when event_type = 'RETENTION_OFFER_SENT' then 1 end) as retention_campaigns_sent,
        count(case when event_type = 'CUSTOMER_SAVED' then 1 end) as retention_calls_made,
        count(case when event_type = 'RETENTION_OFFER_SENT' then 1 end) as retention_offers_extended,
        count(case when event_type = 'CUSTOMER_SAVED' then 1 end) as successful_retentions,
        count(case when event_type = 'ACCOUNT_CLOSED' then 1 end) as churn_events,
        max(event_date) as last_retention_event_date,
        min(event_date) as first_retention_event_date,
        max(case when event_type = 'CHURN_RISK_IDENTIFIED' then event_date end) as last_churn_risk_date,
        -- Calculate a simple risk score based on event patterns (0-100)
        case 
            when count(case when event_type = 'ACCOUNT_CLOSED' then 1 end) > 0 then 90
            when count(case when event_type = 'CHURN_RISK_IDENTIFIED' then 1 end) > 2 then 75
            when count(case when event_type = 'CHURN_RISK_IDENTIFIED' then 1 end) > 0 then 50
            else 25
        end as avg_churn_risk_score
    from {{ ref('stg_retention_events') }}
    group by customer_id
),

customer_interaction_patterns as (
    select
        customer_id,
        count(*) as total_interactions,
        count(case when reason_code = 'FEES_COMPLAINT' then 1 end) as complaint_interactions,
        count(case when reason_code = 'TECHNICAL_SUPPORT' then 1 end) as support_interactions,
        count(case when was_escalated then 1 end) as escalated_interactions,
        avg(case when satisfaction_score is not null then satisfaction_score end) as avg_satisfaction_score,
        min(case when satisfaction_score is not null then satisfaction_score end) as min_satisfaction_score,
        max(interaction_datetime::date) as last_interaction_date,
        count(case when interaction_datetime::date >= current_date - interval '30 days' then 1 end) as interactions_last_30_days,
        count(case when interaction_datetime::date >= current_date - interval '90 days' then 1 end) as interactions_last_90_days
    from {{ ref('stg_customer_interactions') }}
    group by customer_id
),

digital_engagement_trends as (
    select
        customer_id,
        count(*) as total_digital_activities,
        count(case when activity_timestamp >= current_date - interval '30 days' then 1 end) as digital_activities_last_30_days,
        count(case when activity_timestamp >= current_date - interval '90 days' then 1 end) as digital_activities_last_90_days,
        max(activity_timestamp::date) as last_digital_activity_date,
        count(case when activity_type = 'LOGIN' then 1 end) as total_logins,
        count(case when activity_type = 'LOGIN' and activity_timestamp >= current_date - interval '30 days' then 1 end) as logins_last_30_days
    from {{ ref('stg_digital_activity') }}
    group by customer_id
),

customer_profile_insights as (
    select
        customer_id,
        engagement_level,
        at_risk_customer,
        digital_preference,
        avg_satisfaction_score as profile_satisfaction_score,
        relationship_years,
        customer_value_segment,
        lifecycle_stage_name,
        total_service_interactions,
        total_digital_activities
    from {{ ref('int_customer_profile') }}
),

risk_profile_insights as (
    select
        customer_id,
        overall_risk_level,
        compliance_status,
        composite_risk_score,
        total_fraud_alerts,
        requires_enhanced_monitoring,
        requires_immediate_review,
        risk_trend
    from {{ ref('int_comprehensive_risk_profile') }}
)

select
    -- Customer identifier
    coalesce(
        res.customer_id,
        cip.customer_id,
        det.customer_id,
        cpi.customer_id,
        rpi.customer_id
    ) as customer_id,
    
    -- Retention Event History
    coalesce(res.total_retention_events, 0) as total_retention_events,
    coalesce(res.churn_risk_events, 0) as churn_risk_events,
    coalesce(res.retention_campaigns_sent, 0) as retention_campaigns_sent,
    coalesce(res.retention_calls_made, 0) as retention_calls_made,
    coalesce(res.retention_offers_extended, 0) as retention_offers_extended,
    coalesce(res.successful_retentions, 0) as successful_retentions,
    coalesce(res.churn_events, 0) as churn_events,
    res.last_retention_event_date,
    res.last_churn_risk_date,
    coalesce(res.avg_churn_risk_score, 0) as avg_churn_risk_score,
    
    -- Customer Interaction Patterns
    coalesce(cip.total_interactions, 0) as total_interactions,
    coalesce(cip.complaint_interactions, 0) as complaint_interactions,
    coalesce(cip.support_interactions, 0) as support_interactions,
    coalesce(cip.escalated_interactions, 0) as escalated_interactions,
    coalesce(cip.avg_satisfaction_score, 0) as avg_satisfaction_score,
    cip.min_satisfaction_score,
    cip.last_interaction_date,
    coalesce(cip.interactions_last_30_days, 0) as interactions_last_30_days,
    coalesce(cip.interactions_last_90_days, 0) as interactions_last_90_days,
    
    -- Digital Engagement Trends
    coalesce(det.total_digital_activities, 0) as total_digital_activities,
    coalesce(det.digital_activities_last_30_days, 0) as digital_activities_last_30_days,
    coalesce(det.digital_activities_last_90_days, 0) as digital_activities_last_90_days,
    det.last_digital_activity_date,
    coalesce(det.total_logins, 0) as total_logins,
    coalesce(det.logins_last_30_days, 0) as logins_last_30_days,
    
    -- Customer Profile Context
    coalesce(cpi.engagement_level, 'UNKNOWN') as engagement_level,
    coalesce(cpi.at_risk_customer, false) as at_risk_customer,
    coalesce(cpi.digital_preference, 'UNKNOWN') as digital_preference,
    coalesce(cpi.relationship_years, 0) as relationship_years,
    coalesce(cpi.customer_value_segment, 'UNKNOWN') as customer_value_segment,
    coalesce(cpi.lifecycle_stage_name, 'UNKNOWN') as lifecycle_stage_name,
    
    -- Risk Context
    coalesce(rpi.overall_risk_level, 'UNKNOWN') as overall_risk_level,
    coalesce(rpi.compliance_status, 'UNKNOWN') as compliance_status,
    coalesce(rpi.composite_risk_score, 500) as composite_risk_score,
    coalesce(rpi.total_fraud_alerts, 0) as total_fraud_alerts,
    coalesce(rpi.requires_enhanced_monitoring, false) as requires_enhanced_monitoring,
    coalesce(rpi.risk_trend, 'STABLE') as risk_trend,
    
    -- Retention Performance Metrics
    
    -- Retention Campaign Success Rate
    case 
        when res.retention_campaigns_sent > 0 then
            round(res.successful_retentions::numeric / res.retention_campaigns_sent * 100, 2)
        else 0
    end as retention_campaign_success_rate,
    
    -- Churn Risk Score (0-100, higher = more likely to churn)
    least(100, greatest(0,
        -- Historical churn events
        (coalesce(res.churn_events, 0) * 30) +
        -- Current risk flags
        (case when cpi.at_risk_customer then 25 else 0 end) +
        -- Satisfaction issues
        (case 
            when cip.min_satisfaction_score <= 2 then 20
            when cip.avg_satisfaction_score <= 2.5 then 15
            when cip.avg_satisfaction_score <= 3 then 10
            else 0
        end) +
        -- Engagement decline
        (case 
            when det.digital_activities_last_30_days = 0 and det.total_digital_activities > 0 then 15
            when det.digital_activities_last_30_days < det.digital_activities_last_90_days / 3 then 10
            else 0
        end) +
        -- Complaint pattern
        (case 
            when cip.complaint_interactions > cip.support_interactions then 15
            when cip.escalated_interactions > 2 then 10
            else 0
        end) +
        -- Risk factors
        (case 
            when rpi.overall_risk_level in ('HIGH_RISK', 'CRITICAL_RISK') then 10
            when rpi.requires_immediate_review then 5
            else 0
        end)
    )) as churn_risk_score,
    
    -- Retention Opportunity Score (0-100, higher = better retention opportunity)
    least(100, greatest(0,
        -- Customer value
        (case 
            when cpi.customer_value_segment = 'HIGH_VALUE' then 30
            when cpi.customer_value_segment = 'MEDIUM_VALUE' then 20
            when cpi.customer_value_segment = 'LOW_VALUE' then 10
            else 5
        end) +
        -- Relationship tenure
        (case 
            when cpi.relationship_years >= 5 then 25
            when cpi.relationship_years >= 2 then 20
            when cpi.relationship_years >= 1 then 15
            else 10
        end) +
        -- Historical retention success
        (case 
            when res.successful_retentions > 0 then 20
            when res.retention_campaigns_sent > 0 then 10
            else 0
        end) +
        -- Engagement level
        (case 
            when cpi.engagement_level = 'HIGHLY_ENGAGED' then 15
            when cpi.engagement_level = 'MODERATELY_ENGAGED' then 10
            when cpi.engagement_level = 'LIGHTLY_ENGAGED' then 5
            else 0
        end) +
        -- Digital adoption
        (case 
            when cpi.digital_preference in ('DIGITAL_FIRST', 'DIGITAL_PREFERRED') then 10
            when cpi.digital_preference = 'MULTI_CHANNEL' then 5
            else 0
        end)
    )) as retention_opportunity_score,
    
    -- Customer Lifecycle Stage for Retention
    case 
        when res.churn_events > 0 then 'CHURNED'
        when res.churn_risk_events > 0 and res.last_churn_risk_date >= current_date - interval '30 days' then 'HIGH_CHURN_RISK'
        when cip.avg_satisfaction_score <= 2 or cip.escalated_interactions > 2 then 'DISSATISFIED'
        when det.digital_activities_last_30_days = 0 and det.total_digital_activities > 0 then 'DISENGAGED'
        when cip.interactions_last_30_days > 3 and cip.complaint_interactions > 0 then 'COMPLAINT_PATTERN'
        when res.successful_retentions > 0 then 'SUCCESSFULLY_RETAINED'
        when cpi.engagement_level = 'HIGHLY_ENGAGED' then 'LOYAL'
        else 'STABLE'
    end as retention_lifecycle_stage,
    
    -- Next Best Retention Action
    case 
        when res.churn_events > 0 then 'WIN_BACK_CAMPAIGN'
        when res.churn_risk_events > 0 and res.last_churn_risk_date >= current_date - interval '30 days' then 'IMMEDIATE_RETENTION_CALL'
        when cip.min_satisfaction_score <= 2 then 'SATISFACTION_RECOVERY'
        when det.digital_activities_last_30_days = 0 and det.total_digital_activities > 0 then 'RE_ENGAGEMENT_CAMPAIGN'
        when cip.complaint_interactions > cip.support_interactions then 'PROACTIVE_OUTREACH'
        when cip.escalated_interactions > 1 then 'EXECUTIVE_ESCALATION'
        when cpi.engagement_level = 'MINIMAL_ENGAGED' then 'ACTIVATION_CAMPAIGN'
        else 'STANDARD_RETENTION'
    end as next_best_retention_action,
    
    -- Retention Strategy Recommendation
    case 
        when cpi.customer_value_segment = 'HIGH_VALUE' and res.churn_risk_events > 0 then 'CONCIERGE_RETENTION'
        when cpi.digital_preference = 'DIGITAL_FIRST' then 'DIGITAL_RETENTION_CAMPAIGN'
        when cip.complaint_interactions > 0 then 'SERVICE_RECOVERY'
        when det.digital_activities_last_30_days = 0 then 'PRODUCT_EDUCATION'
        when cpi.relationship_years >= 5 then 'LOYALTY_REWARDS'
        else 'STANDARD_RETENTION'
    end as retention_strategy,
    
    -- Retention Priority Level
    case 
        when res.churn_events > 0 then 'WIN_BACK'
        when res.churn_risk_events > 0 and cpi.customer_value_segment = 'HIGH_VALUE' then 'CRITICAL'
        when res.churn_risk_events > 0 or cip.avg_satisfaction_score <= 2 then 'HIGH'
        when cip.escalated_interactions > 1 or det.digital_activities_last_30_days = 0 then 'MEDIUM'
        when cpi.engagement_level = 'MINIMAL_ENGAGED' then 'LOW'
        else 'MONITOR'
    end as retention_priority,
    
    current_timestamp as last_updated

from retention_events_summary res
full outer join customer_interaction_patterns cip on res.customer_id = cip.customer_id
full outer join digital_engagement_trends det on coalesce(res.customer_id, cip.customer_id) = det.customer_id
full outer join customer_profile_insights cpi on coalesce(res.customer_id, cip.customer_id, det.customer_id) = cpi.customer_id
full outer join risk_profile_insights rpi on coalesce(res.customer_id, cip.customer_id, det.customer_id, cpi.customer_id) = rpi.customer_id 
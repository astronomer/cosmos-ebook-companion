{{ config(
    materialized='view',
    tags=['silver', 'intermediate', 'product_analytics', 'channels']
) }}

/*
    Channel Effectiveness Analytics
    
    Comprehensive channel performance analysis combining:
    - Channel usage patterns (stg_channel_usage)
    - Marketing campaign effectiveness (stg_marketing_campaigns)
    - Customer service interactions (stg_customer_interactions)
    - Digital activity patterns (stg_digital_activity)
    
    Provides insights for channel optimization and omnichannel strategy.
*/

with channel_usage_metrics as (
    select
        customer_id,
        channel_type,
        sum(usage_count) as total_usage,
        avg(usage_count) as avg_usage,
        max(usage_date) as last_used_date,
        min(usage_date) as first_used_date,
        count(distinct extract(month from usage_date)) as months_active
    from {{ ref('stg_channel_usage') }}
    group by customer_id, channel_type
),

customer_channel_summary as (
    select
        customer_id,
        count(distinct channel_type) as total_channels_used,
        count(distinct case when channel_type in ('ONLINE', 'MOBILE') then channel_type end) as digital_channels_used,
        count(distinct case when channel_type in ('BRANCH', 'ATM', 'PHONE') then channel_type end) as physical_channels_used,
        sum(total_usage) as total_channel_usage,
        sum(case when channel_type in ('ONLINE', 'MOBILE') then total_usage else 0 end) as digital_channel_usage,
        sum(case when channel_type in ('BRANCH', 'ATM', 'PHONE') then total_usage else 0 end) as physical_channel_usage,
        max(last_used_date) as last_channel_usage_date
    from channel_usage_metrics
    group by customer_id
),

marketing_campaign_effectiveness as (
    select
        customer_id,
        channel,
        count(*) as campaigns_received,
        count(case when response_type = 'OPENED' then 1 end) as campaigns_opened,
        count(case when response_type = 'CLICKED' then 1 end) as campaigns_clicked,
        count(case when response_type = 'CONVERTED' then 1 end) as campaigns_converted,
        sum(case when response_type = 'CONVERTED' then 
            case 
                when campaign_type = 'CREDIT_CARD_OFFER' then 500
                when campaign_type = 'SAVINGS_PROMOTION' then 200
                when campaign_type = 'LOAN_OFFER' then 1000
                when campaign_type = 'INVESTMENT_SEMINAR' then 750
                when campaign_type = 'INSURANCE_QUOTE' then 300
                when campaign_type = 'DIGITAL_BANKING' then 100
                else 250
            end
        else 0 end) as total_conversion_value,
        avg(case when response_type = 'CONVERTED' then 
            case 
                when campaign_type = 'CREDIT_CARD_OFFER' then 500
                when campaign_type = 'SAVINGS_PROMOTION' then 200
                when campaign_type = 'LOAN_OFFER' then 1000
                when campaign_type = 'INVESTMENT_SEMINAR' then 750
                when campaign_type = 'INSURANCE_QUOTE' then 300
                when campaign_type = 'DIGITAL_BANKING' then 100
                else 250
            end
        else 0 end) as avg_conversion_value
    from {{ ref('stg_marketing_campaigns') }}
    group by customer_id, channel
),

customer_marketing_summary as (
    select
        customer_id,
        count(distinct channel) as channels_marketed_through,
        sum(campaigns_received) as total_campaigns_received,
        sum(campaigns_converted) as total_campaigns_converted,
        sum(total_conversion_value) as total_marketing_value,
        avg(case when campaigns_received > 0 then campaigns_converted::numeric / campaigns_received end) as avg_conversion_rate
    from marketing_campaign_effectiveness
    group by customer_id
),

service_interaction_channels as (
    select
        customer_id,
        interaction_type,
        count(*) as interactions_count,
        avg(case when satisfaction_score is not null then satisfaction_score end) as avg_satisfaction_score,
        count(case when was_escalated then 1 end) as escalated_interactions,
        count(case when status = 'RESOLVED' then 1 end) as resolved_interactions
    from {{ ref('stg_customer_interactions') }}
    group by customer_id, interaction_type
),

customer_service_summary as (
    select
        customer_id,
        count(distinct interaction_type) as service_channels_used,
        sum(interactions_count) as total_service_interactions,
        avg(avg_satisfaction_score) as overall_service_satisfaction,
        sum(escalated_interactions) as total_escalated_interactions,
        sum(resolved_interactions) as total_resolved_interactions
    from service_interaction_channels
    group by customer_id
),

digital_platform_usage as (
    select
        customer_id,
        platform,
        count(*) as activities_count,
        count(distinct activity_type) as activity_types_used,
        max(activity_timestamp::date) as last_activity_date
    from {{ ref('stg_digital_activity') }}
    group by customer_id, platform
),

customer_digital_summary as (
    select
        customer_id,
        count(distinct platform) as digital_platforms_used,
        sum(activities_count) as total_digital_activities,
        max(last_activity_date) as last_digital_activity_date
    from digital_platform_usage
    group by customer_id
)

select
    -- Customer identifier
    coalesce(
        ccs.customer_id,
        cms.customer_id,
        css.customer_id,
        cds.customer_id
    ) as customer_id,
    
    -- Channel Usage Overview
    coalesce(ccs.total_channels_used, 0) as total_channels_used,
    coalesce(ccs.digital_channels_used, 0) as digital_channels_used,
    coalesce(ccs.physical_channels_used, 0) as physical_channels_used,
    coalesce(ccs.total_channel_usage, 0) as total_channel_usage,
    coalesce(ccs.digital_channel_usage, 0) as digital_channel_usage,
    coalesce(ccs.physical_channel_usage, 0) as physical_channel_usage,
    ccs.last_channel_usage_date,
    
    -- Marketing Channel Performance
    coalesce(cms.channels_marketed_through, 0) as marketing_channels_used,
    coalesce(cms.total_campaigns_received, 0) as total_campaigns_received,
    coalesce(cms.total_campaigns_converted, 0) as total_campaigns_converted,
    coalesce(cms.total_marketing_value, 0) as total_marketing_value,
    coalesce(cms.avg_conversion_rate, 0) as marketing_conversion_rate,
    
    -- Service Channel Performance
    coalesce(css.service_channels_used, 0) as service_channels_used,
    coalesce(css.total_service_interactions, 0) as total_service_interactions,
    coalesce(css.overall_service_satisfaction, 0) as service_satisfaction_score,
    coalesce(css.total_escalated_interactions, 0) as total_escalated_interactions,
    coalesce(css.total_resolved_interactions, 0) as total_resolved_interactions,
    
    -- Digital Platform Usage
    coalesce(cds.digital_platforms_used, 0) as digital_platforms_used,
    coalesce(cds.total_digital_activities, 0) as total_digital_activities,
    cds.last_digital_activity_date,
    
    -- Channel Effectiveness Metrics
    
    -- Digital Adoption Score (0-100)
    least(100, greatest(0,
        (coalesce(ccs.digital_channels_used, 0) * 15) +
        (coalesce(cds.digital_platforms_used, 0) * 20) +
        (case 
            when ccs.total_channel_usage > 0 then 
                round(ccs.digital_channel_usage::numeric / ccs.total_channel_usage * 65)
            else 0
        end)
    )) as digital_adoption_score,
    
    -- Channel Diversity Score (0-100)
    least(100, greatest(0,
        (coalesce(ccs.total_channels_used, 0) * 12) +
        (coalesce(cms.channels_marketed_through, 0) * 8) +
        (coalesce(css.service_channels_used, 0) * 10) +
        (coalesce(cds.digital_platforms_used, 0) * 15) +
        (case 
            when ccs.digital_channels_used > 0 and ccs.physical_channels_used > 0 then 20
            when ccs.digital_channels_used > 0 or ccs.physical_channels_used > 0 then 10
            else 0
        end)
    )) as channel_diversity_score,
    
    -- Channel Preference Classification
    case 
        when ccs.digital_channel_usage > ccs.physical_channel_usage * 4 then 'DIGITAL_ONLY'
        when ccs.digital_channel_usage > ccs.physical_channel_usage * 2 then 'DIGITAL_FIRST'
        when ccs.digital_channel_usage > ccs.physical_channel_usage then 'DIGITAL_PREFERRED'
        when ccs.physical_channel_usage > ccs.digital_channel_usage then 'PHYSICAL_PREFERRED'
        when ccs.digital_channel_usage > 0 and ccs.physical_channel_usage > 0 then 'OMNI_CHANNEL'
        when ccs.digital_channel_usage > 0 then 'DIGITAL_ONLY'
        when ccs.physical_channel_usage > 0 then 'PHYSICAL_ONLY'
        else 'MINIMAL_USAGE'
    end as channel_preference,
    
    -- Service Channel Effectiveness
    case 
        when css.total_service_interactions > 0 then
            round(css.total_resolved_interactions::numeric / css.total_service_interactions * 100, 2)
        else 0
    end as service_resolution_rate,
    
    -- Marketing Channel ROI Indicator
    case 
        when cms.total_campaigns_received > 0 then
            round(cms.total_marketing_value / cms.total_campaigns_received, 2)
        else 0
    end as marketing_roi_per_campaign,
    
    -- Channel Engagement Level
    case 
        when ccs.total_channel_usage >= 100 then 'VERY_HIGH'
        when ccs.total_channel_usage >= 50 then 'HIGH'
        when ccs.total_channel_usage >= 20 then 'MEDIUM'
        when ccs.total_channel_usage >= 5 then 'LOW'
        when ccs.total_channel_usage > 0 then 'MINIMAL'
        else 'NONE'
    end as channel_engagement_level,
    
    -- Digital Session Quality
    case 
        when cds.total_digital_activities >= 50 then 'HIGH_QUALITY'
        when cds.total_digital_activities >= 20 then 'MEDIUM_QUALITY'
        when cds.total_digital_activities >= 5 then 'LOW_QUALITY'
        when cds.total_digital_activities > 0 then 'BRIEF_SESSIONS'
        else 'NO_SESSIONS'
    end as digital_session_quality,
    
    -- Next Best Channel Action
    case 
        when ccs.digital_channels_used = 0 and ccs.physical_channels_used > 0 then 'DIGITAL_ONBOARDING'
        when cds.total_digital_activities = 0 and ccs.total_channels_used = 1 then 'CHANNEL_EXPANSION'
        when css.total_escalated_interactions > 2 then 'SERVICE_CHANNEL_OPTIMIZATION'
        when cms.avg_conversion_rate < 0.05 and cms.total_campaigns_received > 10 then 'MARKETING_CHANNEL_REVIEW'
        else 'MAINTAIN_CURRENT_STRATEGY'
    end as next_best_channel_action,
    
    -- Channel Optimization Opportunity
    case 
        when ccs.digital_channels_used = 0 and ccs.physical_channels_used > 0 then 'HIGH_DIGITAL_OPPORTUNITY'
        when cms.avg_conversion_rate = 0 and cms.total_campaigns_received > 5 then 'MARKETING_OPTIMIZATION'
        when css.service_channels_used > 3 and css.overall_service_satisfaction < 3 then 'SERVICE_CONSOLIDATION'
        when cds.digital_platforms_used = 1 and cds.total_digital_activities > 20 then 'PLATFORM_EXPANSION'
        else 'STANDARD_OPTIMIZATION'
    end as channel_optimization_opportunity,
    
    current_timestamp as last_updated

from customer_channel_summary ccs
full outer join customer_marketing_summary cms on ccs.customer_id = cms.customer_id
full outer join customer_service_summary css on coalesce(ccs.customer_id, cms.customer_id) = css.customer_id
full outer join customer_digital_summary cds on coalesce(ccs.customer_id, cms.customer_id, css.customer_id) = cds.customer_id 
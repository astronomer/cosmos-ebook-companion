{{ config(
    materialized='view',
    tags=['silver', 'intermediate', 'customer_analytics', 'customer_profile']
) }}

/*
    Intermediate Customer Profile – Snowflake-compatible

    Combines demographics, contact, segmentation, interaction, and digital activity
    for downstream personalization and analytics models.
*/

with deduplicated_customers as (
    select distinct
        customer_id,
        customer_number,
        full_name,
        first_name,
        last_name,
        age,
        gender,
        annual_income,
        credit_score,
        customer_status,
        customer_since_date,
        relationship_years,
        age_cohort_name,
        income_bracket_name,
        credit_score_range_name,
        risk_level,
        lifecycle_stage_name,
        customer_value_segment,
        marketing_eligible,
        lending_eligible,
        premium_product_eligible
    from {{ ref('stg_customers') }}
),

one_address_per_customer as (
    select
        customer_id,
        street_address,
        city,
        state_code,
        postal_code,
        full_address,
        address_status as primary_address_status,
        case when address_status = 'VERIFIED' then true else false end as address_verified
    from (
        select *,
               row_number() over (
                   partition by customer_id
                   order by 
                       case when address_type = 'PRIMARY' then 1 else 2 end,
                       address_since desc
               ) as rn
        from {{ ref('stg_customer_addresses') }}
    ) ranked_addresses
    where rn = 1
),

one_phone_per_customer as (
    select
        customer_id,
        formatted_phone as primary_phone,
        phone_status as primary_phone_status,
        case when phone_status = 'VERIFIED' then true else false end as phone_verified,
        communication_ready as call_eligible,
        sms_ready as sms_eligible
    from (
        select *,
               row_number() over (
                   partition by customer_id
                   order by 
                       case when is_primary then 1 else 2 end,
                       phone_id desc
               ) as rn
        from {{ ref('stg_customer_phones') }}
    ) ranked_phones
    where rn = 1
),

one_segment_per_customer as (
    select
        customer_id,
        segment_name as current_segment,
        segment_date as segment_assigned_date
    from (
        select *,
               row_number() over (
                   partition by customer_id
                   order by segment_date desc
               ) as rn
        from {{ ref('stg_customer_segments') }}
    ) ranked_segments
    where rn = 1
),

interaction_summary as (
    select
        customer_id,
        count(*) as total_interactions,
        count(case when status = 'RESOLVED' then 1 end) as resolved_interactions,
        count(case when was_escalated then 1 end) as escalated_interactions,
        avg(satisfaction_score) as avg_satisfaction_score,
        max(cast(interaction_datetime as date)) as last_interaction_date,
        min(cast(interaction_datetime as date)) as first_interaction_date,
        count(distinct interaction_type) as interaction_types_used,
        sum(case when resulted_in_retention then 1 else 0 end) as retention_interactions
    from {{ ref('stg_customer_interactions') }}
    group by customer_id
),

digital_engagement as (
    select
        customer_id,
        count(*) as total_digital_activities,
        count(case when platform = 'MOBILE_APP' then 1 end) as mobile_activities,
        count(case when platform = 'WEB_PORTAL' then 1 end) as web_activities,
        count(distinct activity_type) as unique_activity_types,
        max(cast(activity_timestamp as date)) as last_digital_activity_date,
        count(case when activity_type = 'LOGIN' then 1 end) as login_count,
        count(case when activity_type in ('TRANSFER', 'BILL_PAY', 'MOBILE_DEPOSIT') then 1 end) as financial_transaction_count
    from {{ ref('stg_digital_activity') }}
    group by customer_id
)

select distinct
    -- Customer Identity
    cb.customer_id,
    cb.customer_number,
    cb.full_name,
    cb.first_name,
    cb.last_name,

    -- Demographics
    cb.age,
    cb.gender,
    cb.age_cohort_name,

    -- Financial Profile
    cb.annual_income,
    cb.credit_score,
    cb.income_bracket_name,
    cb.credit_score_range_name,
    cb.risk_level,

    -- Contact Information
    pa.full_address as primary_address,
    pa.city,
    pa.state_code,
    pa.postal_code as zip_code,
    pa.address_verified,
    pp.primary_phone,
    pp.phone_verified,
    pp.call_eligible,
    pp.sms_eligible,

    -- Customer Lifecycle
    cb.customer_status,
    cb.customer_since_date,
    cb.relationship_years,
    cb.lifecycle_stage_name,
    cb.customer_value_segment,
    cs.current_segment,
    cs.segment_assigned_date,

    -- Eligibility Flags
    cb.marketing_eligible,
    cb.lending_eligible,
    cb.premium_product_eligible,

    -- Service Interaction Profile
    coalesce(is_summary.total_interactions, 0)           as total_service_interactions,
    coalesce(is_summary.resolved_interactions, 0)        as resolved_service_interactions,
    coalesce(is_summary.escalated_interactions, 0)       as escalated_interactions,
    is_summary.avg_satisfaction_score,
    is_summary.last_interaction_date,
    coalesce(is_summary.retention_interactions, 0)       as retention_interactions,

    -- Digital Engagement Profile
    coalesce(de.total_digital_activities, 0)             as total_digital_activities,
    coalesce(de.mobile_activities, 0)                    as mobile_app_activities,
    coalesce(de.web_activities, 0)                       as web_portal_activities,
    de.last_digital_activity_date,
    coalesce(de.login_count, 0)                          as digital_login_count,
    coalesce(de.financial_transaction_count, 0)          as digital_financial_transactions,

    -- Engagement Scoring
    case 
        when de.total_digital_activities >= 50 and is_summary.avg_satisfaction_score >= 4 then 'HIGHLY_ENGAGED'
        when de.total_digital_activities >= 20 and is_summary.avg_satisfaction_score >= 3 then 'MODERATELY_ENGAGED'
        when coalesce(de.total_digital_activities, 0) > 0 or coalesce(is_summary.total_interactions, 0) > 0 then 'LIGHTLY_ENGAGED'
        else 'MINIMAL_ENGAGEMENT'
    end as engagement_level,

    -- Risk Indicators
    case 
        when coalesce(is_summary.escalated_interactions, 0) > 2 or is_summary.avg_satisfaction_score < 2 then true
        else false
    end as at_risk_customer,

    -- Digital Adoption
    case 
        when de.mobile_activities > de.web_activities then 'MOBILE_PREFERRED'
        when de.web_activities > de.mobile_activities then 'WEB_PREFERRED'
        when coalesce(de.total_digital_activities, 0) > 0 then 'MULTI_CHANNEL'
        else 'NON_DIGITAL'
    end as digital_preference,

    current_timestamp() as last_updated

from deduplicated_customers cb
left join one_address_per_customer pa       on cb.customer_id = pa.customer_id
left join one_phone_per_customer pp         on cb.customer_id = pp.customer_id  
left join one_segment_per_customer cs       on cb.customer_id = cs.customer_id
left join interaction_summary is_summary    on cb.customer_id = is_summary.customer_id
left join digital_engagement de             on cb.customer_id = de.customer_id
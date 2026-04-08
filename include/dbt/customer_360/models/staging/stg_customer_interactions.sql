{{ config(
    materialized='view',
    tags=['bronze', 'staging', 'customer_service', 'interactions']
) }}

/*
    Staging model for customer service interactions
    
    Tracks all customer touchpoints across channels
    with resolution metrics and satisfaction scoring.
*/

with mock_interactions as (
    select
        series_value as interaction_id,
        'INT' || lpad(series_value::text, 8, '0') as interaction_number,
        
        -- Link to customers 
        ((series_value - 1) % {{ var('num_customers') }}) + 1 as customer_id,
        
        -- Interaction types
        case 
            when series_value % 5 = 1 then 'PHONE_CALL'
            when series_value % 5 = 2 then 'EMAIL'
            when series_value % 5 = 3 then 'CHAT'
            when series_value % 5 = 4 then 'BRANCH_VISIT'
            else 'SOCIAL_MEDIA'
        end as interaction_type,
        
        -- Reason codes
        case 
            when series_value % 8 = 1 then 'ACCOUNT_INQUIRY'
            when series_value % 8 = 2 then 'TRANSACTION_DISPUTE'
            when series_value % 8 = 3 then 'CARD_ISSUE'
            when series_value % 8 = 4 then 'LOAN_QUESTION'
            when series_value % 8 = 5 then 'FEES_COMPLAINT'
            when series_value % 8 = 6 then 'TECHNICAL_SUPPORT'
            when series_value % 8 = 7 then 'PRODUCT_INFORMATION'
            else 'GENERAL_INQUIRY'
        end as reason_code,
        
        -- Status
        case 
            when series_value % 20 = 1 then 'ESCALATED'
            when series_value % 15 = 1 then 'PENDING'
            when series_value % 10 = 1 then 'REOPENED'
            else 'RESOLVED'
        end as status,
        
        -- Priority
        case 
            when series_value % 20 = 1 then 'URGENT'
            when series_value % 10 = 1 then 'HIGH'
            when series_value % 5 = 1 then 'MEDIUM'
            else 'LOW'
        end as priority,
        
        -- Timing (distributed over last year)
        '2024-01-01'::timestamp + (series_value % 365) * interval '1 day' + 
        ((series_value % 8) + 8) * interval '1 hour' + 
        (series_value % 60) * interval '1 minute' as interaction_datetime,
        
        -- Duration in minutes
        case 
            when series_value % 5 = 1 then (series_value % 45) + 5    -- Phone: 5-50 mins
            when series_value % 5 = 2 then (series_value % 1440) + 60 -- Email: 1-24 hours response
            when series_value % 5 = 3 then (series_value % 30) + 5    -- Chat: 5-35 mins
            when series_value % 5 = 4 then (series_value % 60) + 15   -- Branch: 15-75 mins
            else (series_value % 120) + 30                            -- Social: 30-150 mins
        end as duration_minutes,
        
        -- Agent info
        'AGENT' || lpad(((series_value % 25) + 1)::text, 3, '0') as agent_id,
        case 
            when series_value % 4 = 1 then 'TIER_1'
            when series_value % 4 = 2 then 'TIER_2'
            when series_value % 4 = 3 then 'SPECIALIST'
            else 'SUPERVISOR'
        end as agent_tier,
        
        -- Resolution info
        case 
            when series_value % 10 = 1 then (series_value % 5) + 1  -- Multiple contacts
            else 1
        end as contact_count,
        
        -- Satisfaction (1-5 scale, not always provided)
        case 
            when series_value % 3 = 1 then (series_value % 5) + 1
            else null
        end as satisfaction_score,
        
        -- Flags
        case when series_value % 15 = 1 then true else false end as was_escalated,
        case when series_value % 12 = 1 then true else false end as required_followup,
        case when series_value % 8 = 1 then true else false end as resulted_in_retention,
        
        current_timestamp as last_updated
        
    from generate_series(1, ({{ var('num_customers') }} * 8)::int) as series_value
),

base_interactions as (
    select
        interaction_id,
        interaction_number,
        customer_id,
        
        -- Interaction details
        upper(interaction_type) as interaction_type,
        upper(reason_code) as reason_code,
        upper(status) as status,
        upper(priority) as priority,
        
        -- Timing
        interaction_datetime,
        interaction_datetime::date as interaction_date,
        extract(hour from interaction_datetime) as interaction_hour,
        extract(dow from interaction_datetime) as day_of_week,
        duration_minutes,
        
        -- Agent info
        agent_id,
        upper(agent_tier) as agent_tier,
        
        -- Resolution metrics
        contact_count,
        satisfaction_score,
        
        -- Flags
        was_escalated,
        required_followup,
        resulted_in_retention,
        
        -- Calculated fields
        case 
            when duration_minutes <= 15 then 'QUICK'
            when duration_minutes <= 60 then 'STANDARD'
            when duration_minutes <= 120 then 'EXTENDED'
            else 'LENGTHY'
        end as duration_category,
        
        last_updated,
        current_timestamp as dbt_created_at
        
    from mock_interactions
    where status != 'INVALID'
),

enriched_interactions as (
    select
        bi.*,
        
        -- Channel analysis
        case 
            when bi.interaction_type in ('EMAIL', 'CHAT', 'SOCIAL_MEDIA') then 'DIGITAL'
            when bi.interaction_type = 'PHONE_CALL' then 'VOICE'
            when bi.interaction_type = 'BRANCH_VISIT' then 'IN_PERSON'
            else 'OTHER'
        end as channel_category,
        
        -- Time-based patterns
        case 
            when bi.interaction_hour between 9 and 17 then 'BUSINESS_HOURS'
            when bi.interaction_hour between 18 and 22 then 'EVENING'
            when bi.interaction_hour between 6 and 8 then 'MORNING'
            else 'OFF_HOURS'
        end as time_category,
        
        case 
            when bi.day_of_week in (1, 7) then 'WEEKEND'
            else 'WEEKDAY'
        end as day_category,
        
        -- Effort scoring
        case 
            when bi.contact_count > 3 then 'HIGH_EFFORT'
            when bi.contact_count > 1 then 'MEDIUM_EFFORT'
            else 'LOW_EFFORT'
        end as customer_effort,
        
        -- Service quality indicators
        case 
            when bi.satisfaction_score >= 4 then 'SATISFIED'
            when bi.satisfaction_score >= 3 then 'NEUTRAL'
            when bi.satisfaction_score >= 1 then 'DISSATISFIED'
            else 'NOT_RATED'
        end as satisfaction_category,
        
        -- Resolution efficiency
        case 
            when bi.status = 'RESOLVED' and bi.contact_count = 1 and not bi.was_escalated then 'FIRST_CONTACT_RESOLUTION'
            when bi.status = 'RESOLVED' and not bi.was_escalated then 'RESOLVED_NO_ESCALATION'
            when bi.status = 'RESOLVED' and bi.was_escalated then 'RESOLVED_WITH_ESCALATION'
            when bi.status = 'ESCALATED' then 'UNRESOLVED_ESCALATED'
            else 'UNRESOLVED_PENDING'
        end as resolution_type,
        
        -- Issue complexity
        case 
            when bi.reason_code in ('TRANSACTION_DISPUTE', 'LOAN_QUESTION') and bi.was_escalated then 'COMPLEX'
            when bi.agent_tier in ('SPECIALIST', 'SUPERVISOR') then 'MODERATE'
            else 'SIMPLE'
        end as complexity_level,
        
        -- Digital adoption indicator
        case 
            when bi.interaction_type in ('EMAIL', 'CHAT', 'SOCIAL_MEDIA') then true 
            else false 
        end as is_digital_interaction,
        
        -- Business impact flags
        case 
            when bi.reason_code = 'FEES_COMPLAINT' and bi.satisfaction_score <= 2 then true
            when bi.priority = 'URGENT' and bi.status != 'RESOLVED' then true
            else false
        end as high_risk_interaction
        
    from base_interactions bi
)

select * from enriched_interactions 
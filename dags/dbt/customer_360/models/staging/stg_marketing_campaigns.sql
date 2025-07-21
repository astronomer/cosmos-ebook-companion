{{ config(
    materialized='view',
    tags=['bronze', 'staging', 'marketing', 'campaigns']
) }}

with mock_campaigns as (
    select
        series_value as campaign_response_id,
        'CAMP' || lpad(((series_value - 1) / {{ var('num_customers') }} + 1)::text, 6, '0') as campaign_id,
        ((series_value - 1) % {{ var('num_customers') }}) + 1 as customer_id,
        
        case 
            when ((series_value - 1) / {{ var('num_customers') }}) % 6 = 0 then 'CREDIT_CARD_OFFER'
            when ((series_value - 1) / {{ var('num_customers') }}) % 6 = 1 then 'SAVINGS_PROMOTION'
            when ((series_value - 1) / {{ var('num_customers') }}) % 6 = 2 then 'LOAN_OFFER'
            when ((series_value - 1) / {{ var('num_customers') }}) % 6 = 3 then 'INVESTMENT_SEMINAR'
            when ((series_value - 1) / {{ var('num_customers') }}) % 6 = 4 then 'INSURANCE_QUOTE'
            else 'DIGITAL_BANKING'
        end as campaign_type,
        
        case 
            when series_value % 4 = 1 then 'EMAIL'
            when series_value % 4 = 2 then 'DIRECT_MAIL'
            when series_value % 4 = 3 then 'PHONE'
            else 'DIGITAL_AD'
        end as channel,
        
        '2024-01-01'::date + (series_value % 365) * interval '1 day' as sent_date,
        
        case 
            when series_value % 3 = 1 then 'OPENED'
            when series_value % 5 = 1 then 'CLICKED'
            when series_value % 10 = 1 then 'CONVERTED'
            else 'NO_RESPONSE'
        end as response_type,
        
        case when series_value % 20 = 1 then true else false end as resulted_in_sale,
        
        current_timestamp as last_updated
        
    from generate_series(1, ({{ var('num_customers') }} * 6)::int) as series_value
),

enriched_campaigns as (
    select
        mc.*,
        
        case 
            when mc.response_type = 'CONVERTED' then 'HIGH_ENGAGEMENT'
            when mc.response_type = 'CLICKED' then 'MEDIUM_ENGAGEMENT'
            when mc.response_type = 'OPENED' then 'LOW_ENGAGEMENT'
            else 'NO_ENGAGEMENT'
        end as engagement_level,
        
        case 
            when mc.channel in ('EMAIL', 'DIGITAL_AD') then 'DIGITAL'
            else 'TRADITIONAL'
        end as channel_type,
        
        current_timestamp as dbt_created_at
        
    from mock_campaigns mc
)

select * from enriched_campaigns 
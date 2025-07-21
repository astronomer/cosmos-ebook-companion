{{ config(
    materialized='view',
    tags=['bronze', 'staging', 'marketing']
) }}

with mock_campaigns as (
    select
        ROW_NUMBER() OVER (ORDER BY NULL) as campaign_id,
        ((ROW_NUMBER() OVER (ORDER BY NULL) - 1) % {{ var('num_customers') }}) + 1 as customer_id,
        
        case 
            when ROW_NUMBER() OVER (ORDER BY NULL) % 5 = 1 then 'EMAIL_PROMOTION'
            when ROW_NUMBER() OVER (ORDER BY NULL) % 5 = 2 then 'DIRECT_MAIL'
            when ROW_NUMBER() OVER (ORDER BY NULL) % 5 = 3 then 'DIGITAL_ADS'
            when ROW_NUMBER() OVER (ORDER BY NULL) % 5 = 4 then 'SOCIAL_MEDIA'
            else 'PHONE_CALL'
        end as campaign_type,
        
        DATEADD(day, -(ROW_NUMBER() OVER (ORDER BY NULL) % 365), '2024-01-01'::date) as campaign_date,
        
        case 
            when ROW_NUMBER() OVER (ORDER BY NULL) % 10 = 1 then 'CONVERTED'
            when ROW_NUMBER() OVER (ORDER BY NULL) % 5 = 1 then 'ENGAGED'
            else 'NO_RESPONSE'
        end as response_status,
        
        current_timestamp as last_updated
        
    from TABLE(GENERATOR(ROWCOUNT => ({{ var('num_customers') }} * 8)::int))
)

select *, current_timestamp as dbt_created_at from mock_campaigns 
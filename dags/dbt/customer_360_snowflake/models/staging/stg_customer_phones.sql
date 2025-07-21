{{ config(
    materialized='view',
    tags=['bronze', 'staging', 'customers', 'phones']
) }}

/*
    Staging model for customer phone information
    
    Handles phone number standardization, validation, and
    preference management for communication channels.
*/

with mock_phones as (
    select
        -- Link to customers
        ROW_NUMBER() OVER (ORDER BY NULL) as phone_id,
        case 
            when ROW_NUMBER() OVER (ORDER BY NULL) <= {{ var('num_customers') }} then ROW_NUMBER() OVER (ORDER BY NULL)  -- Primary phones
            else (ROW_NUMBER() OVER (ORDER BY NULL) % {{ var('num_customers') }}) + 1  -- Some customers have multiple phones
        end as customer_id,
        
        -- Phone type
        case 
            when ROW_NUMBER() OVER (ORDER BY NULL) <= {{ var('num_customers') }} then 'PRIMARY'
            when ROW_NUMBER() OVER (ORDER BY NULL) % 4 = 1 then 'MOBILE'
            when ROW_NUMBER() OVER (ORDER BY NULL) % 4 = 2 then 'HOME'
            when ROW_NUMBER() OVER (ORDER BY NULL) % 4 = 3 then 'WORK'
            else 'EMERGENCY'
        end as phone_type,
        
        -- Phone numbers (formatted)
        case 
            when ROW_NUMBER() OVER (ORDER BY NULL) % 3 = 1 then '555-0' || LPAD(((ROW_NUMBER() OVER (ORDER BY NULL) % 900) + 100)::string, 3, '0')
            when ROW_NUMBER() OVER (ORDER BY NULL) % 3 = 2 then '555-1' || LPAD(((ROW_NUMBER() OVER (ORDER BY NULL) % 900) + 100)::string, 3, '0')
            else '555-2' || LPAD(((ROW_NUMBER() OVER (ORDER BY NULL) % 900) + 100)::string, 3, '0')
        end as phone_number,
        
        -- Phone status
        case 
            when ROW_NUMBER() OVER (ORDER BY NULL) % 15 = 1 then 'INVALID'
            when ROW_NUMBER() OVER (ORDER BY NULL) % 10 = 1 then 'UNVERIFIED'
            else 'VERIFIED'
        end as phone_status,
        
        -- Communication preferences
        case when ROW_NUMBER() OVER (ORDER BY NULL) % 3 != 1 then true else false end as sms_enabled,
        case when ROW_NUMBER() OVER (ORDER BY NULL) % 4 != 1 then true else false end as marketing_consent,
        case when ROW_NUMBER() OVER (ORDER BY NULL) % 2 = 1 then true else false end as is_primary,
        
        current_timestamp as last_updated
        
    from TABLE(GENERATOR(ROWCOUNT => ({{ var('num_customers') }} * {{ var('num_phones_multiplier') }})::int))
),

base_phones as (
    select
        phone_id,
        customer_id,
        UPPER(phone_type) as phone_type,
        
        -- Phone number standardization
        phone_number as formatted_phone,
        regexp_replace(phone_number, '[^0-9]', '') as digits_only,
        
        -- Full formatted phone with country code
        '+1-' || phone_number as international_format,
        
        -- Phone validation
        UPPER(phone_status) as phone_status,
        case 
            when length(regexp_replace(phone_number, '[^0-9]', '')) = 10 then true
            else false
        end as is_valid_format,
        
        -- Communication preferences
        sms_enabled,
        marketing_consent,
        is_primary,
        
        last_updated,
        current_timestamp as dbt_created_at
        
    from mock_phones
    where phone_status != 'INVALID'
),

enriched_phones as (
    select
        bp.*,
        
        -- Phone number analysis
        case 
            when bp.phone_type = 'MOBILE' then true 
            else false 
        end as is_mobile,
        
        case 
            when bp.phone_type = 'PRIMARY' or bp.is_primary then true 
            else false 
        end as is_primary_contact,
        
        -- Communication readiness
        case 
            when bp.phone_status = 'VERIFIED' and bp.is_valid_format then true 
            else false 
        end as communication_ready,
        
        case 
            when bp.sms_enabled and bp.phone_status = 'VERIFIED' then true 
            else false 
        end as sms_ready,
        
        case 
            when bp.marketing_consent and bp.phone_status = 'VERIFIED' then true 
            else false 
        end as marketing_ready,
        
        -- Phone quality scoring
        case 
            when bp.phone_status = 'VERIFIED' and bp.is_valid_format and bp.sms_enabled then 95
            when bp.phone_status = 'VERIFIED' and bp.is_valid_format then 85
            when bp.phone_status = 'VERIFIED' then 70
            when bp.phone_status = 'UNVERIFIED' and bp.is_valid_format then 50
            else 25
        end as phone_quality_score
        
    from base_phones bp
)

select * from enriched_phones 
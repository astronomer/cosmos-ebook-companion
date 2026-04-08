{{ config(
    materialized='view',
    tags=['bronze', 'staging', 'customers', 'contact']
) }}

/*
    Staging model for customer phone information
    
    Handles phone number standardization, validation, and
    communication preference management.
*/

with mock_phones as (
    select
        series_value as phone_id,
        case 
            when series_value <= {{ var('num_customers') }} then series_value  -- Primary phones
            else (series_value % {{ var('num_customers') }}) + 1  -- Additional phones for some customers
        end as customer_id,
        
        -- Phone type
        case 
            when series_value <= {{ var('num_customers') }} then 'MOBILE'
            when series_value % 4 = 1 then 'HOME'
            when series_value % 4 = 2 then 'WORK'
            else 'FAX'
        end as phone_type,
        
        -- Phone number components
        case 
            when series_value % 8 = 1 then '555'
            when series_value % 8 = 2 then '444'
            when series_value % 8 = 3 then '333'
            else '555'
        end as area_code,
        
        lpad(((series_value * 123) % 900 + 100)::text, 3, '0') as exchange,
        lpad(((series_value * 456) % 10000)::text, 4, '0') as line_number,
        
        -- Communication preferences
        case when series_value % 3 != 1 then true else false end as is_verified,
        case when series_value % 5 != 1 then true else false end as sms_enabled,
        case when series_value % 7 != 1 then true else false end as marketing_opt_in,
        case when series_value % 10 = 1 then true else false end as do_not_call,
        
        -- Phone status
        case 
            when series_value % 25 = 1 then 'INACTIVE'
            when series_value % 15 = 1 then 'UNVERIFIED'
            else 'ACTIVE'
        end as phone_status,
        
        -- Primary phone indicator
        case 
            when series_value <= {{ var('num_customers') }} then true  -- Primary phones are preferred
            else series_value % 4 = 1
        end as is_primary,
        
        -- Contact windows
        case 
            when series_value % 3 = 1 then '08:00:00'::time
            when series_value % 3 = 2 then '09:00:00'::time
            else '10:00:00'::time
        end as preferred_contact_start,
        
        case 
            when series_value % 3 = 1 then '20:00:00'::time
            when series_value % 3 = 2 then '18:00:00'::time
            else '19:00:00'::time
        end as preferred_contact_end,
        
        current_timestamp as last_updated
        
    from generate_series(1, ({{ var('num_customers') }} * {{ var('num_phones_multiplier') }})::int) as series_value
),

base_phones as (
    select
        phone_id,
        customer_id,
        upper(phone_type) as phone_type,
        
        -- Standardized phone number
        area_code,
        exchange,
        line_number,
        concat(area_code, '-', exchange, '-', line_number) as formatted_phone,
        concat(area_code, exchange, line_number) as phone_digits_only,
        
        -- Phone validation
        case 
            when area_code in ('000', '111', '999') then false
            when exchange in ('000', '111', '999') then false
            when line_number in ('0000', '1111', '9999') then false
            else true
        end as is_valid_format,
        
        -- Communication settings
        is_verified,
        sms_enabled,
        marketing_opt_in,
        do_not_call,
        upper(phone_status) as phone_status,
        is_primary,
        
        -- Contact timing
        preferred_contact_start,
        preferred_contact_end,
        extract(hour from preferred_contact_end - preferred_contact_start) as contact_window_hours,
        
        -- Communication eligibility (moved here from enriched_phones)
        case 
            when is_verified and upper(phone_status) = 'ACTIVE' and not do_not_call then true 
            else false 
        end as call_eligible,
        
        case 
            when sms_enabled and upper(phone_type) = 'MOBILE' and upper(phone_status) = 'ACTIVE' then true 
            else false 
        end as sms_eligible,
        
        last_updated,
        current_timestamp as dbt_created_at
        
    from mock_phones
    where phone_status != 'INVALID'
),

enriched_phones as (
    select
        bp.*,
        
        -- Phone type classifications
        case 
            when bp.phone_type = 'MOBILE' then true 
            else false 
        end as is_mobile,
        
        case 
            when bp.phone_type in ('HOME', 'WORK') then true 
            else false 
        end as is_landline,
        
        -- Marketing eligibility (now can reference bp.call_eligible and bp.sms_eligible)
        case 
            when bp.marketing_opt_in and bp.call_eligible then true 
            else false 
        end as marketing_call_eligible,
        
        case 
            when bp.marketing_opt_in and bp.sms_eligible then true 
            else false 
        end as marketing_sms_eligible,
        
        -- Quality scoring
        case 
            when bp.is_verified and bp.is_valid_format and bp.phone_status = 'ACTIVE' then 95
            when bp.is_valid_format and bp.phone_status = 'ACTIVE' then 80
            when bp.is_valid_format then 60
            else 30
        end as phone_quality_score,
        
        -- Contact channel preferences
        case 
            when bp.phone_type = 'MOBILE' and bp.sms_enabled then 'SMS_PREFERRED'
            when bp.phone_type = 'MOBILE' then 'MOBILE_CALL'
            when bp.phone_type = 'HOME' then 'HOME_CALL'
            when bp.phone_type = 'WORK' then 'WORK_CALL'
            else 'NO_CONTACT'
        end as preferred_contact_method,
        
        -- Risk indicators
        case 
            when bp.area_code in ('555', '800', '888', '877', '866') then true 
            else false 
        end as is_special_number,
        
        case 
            when not bp.is_verified and bp.phone_status = 'ACTIVE' then true 
            else false 
        end as verification_required
        
    from base_phones bp
)

select * from enriched_phones 
{{ config(
    materialized='view',
    tags=['bronze', 'staging', 'cards', 'credit']
) }}

/*
    Staging model for credit and debit cards
    
    Manages card lifecycle, limits, and usage analytics
    for risk management and customer insights.
*/

with mock_cards as (
    select
        series_value as card_id,
        'CARD' || lpad(series_value::text, 8, '0') as card_number_masked,
        
        -- Link to customers (some customers have multiple cards)
        case 
            when series_value <= {{ var('num_customers') }} then series_value  -- Primary cards
            when series_value <= ({{ var('num_customers') }} * 1.8)::int then (series_value % {{ var('num_customers') }}) + 1
            else (series_value % {{ var('num_customers') }}) + 1
        end as customer_id,
        
        -- Link to accounts for debit cards
        case 
            when series_value % 3 = 1 then ((series_value - 1) % ({{ var('num_customers') }} * {{ var('num_accounts_multiplier') }})::int) + 1
            else null  -- Credit cards don't link to deposit accounts
        end as linked_account_id,
        
        -- Card types
        case 
            when series_value % 3 = 1 then 'DEBIT'
            when series_value % 3 = 2 then 'CREDIT'
            else 'PREPAID'
        end as card_type,
        
        -- Card brands
        case 
            when series_value % 4 = 1 then 'VISA'
            when series_value % 4 = 2 then 'MASTERCARD'
            when series_value % 4 = 3 then 'AMEX'
            else 'DISCOVER'
        end as card_brand,
        
        -- Card status
        case 
            when series_value % 20 = 1 then 'BLOCKED'
            when series_value % 15 = 1 then 'EXPIRED'
            when series_value % 10 = 1 then 'INACTIVE'
            else 'ACTIVE'
        end as card_status,
        
        -- Credit limits (for credit cards)
        case 
            when series_value % 3 = 2 then  -- Credit cards only
                case 
                    when series_value % 10 = 1 then 50000
                    when series_value % 5 = 1 then 25000
                    when series_value % 3 = 1 then 10000
                    else 5000
                end
            else null
        end as credit_limit,
        
        -- Current balances
        case 
            when series_value % 3 = 2 then  -- Credit cards
                (series_value % 5000)::numeric(10,2)
            when series_value % 3 = 0 then  -- Prepaid cards
                (100 + (series_value % 2000))::numeric(10,2)
            else 0  -- Debit cards
        end as current_balance,
        
        -- Issue and expiry dates
        '2020-01-01'::date + (series_value % 1460) * interval '1 day' as issue_date,
        '2020-01-01'::date + (series_value % 1460) * interval '1 day' + interval '4 years' as expiry_date,
        
        -- Usage flags
        case when series_value % 3 != 1 then true else false end as contactless_enabled,
        case when series_value % 4 != 1 then true else false end as online_enabled,
        case when series_value % 5 != 1 then true else false end as international_enabled,
        case when series_value % 8 = 1 then true else false end as is_business_card,
        
        current_timestamp as last_updated
        
    from generate_series(1, ({{ var('num_customers') }} * 1.8)::int) as series_value
),

base_cards as (
    select
        card_id,
        card_number_masked,
        customer_id,
        linked_account_id,
        
        -- Card details
        upper(card_type) as card_type,
        upper(card_brand) as card_brand,
        upper(card_status) as card_status,
        
        -- Financial details
        credit_limit,
        current_balance,
        case 
            when credit_limit > 0 and current_balance > 0 then 
                round((current_balance / credit_limit * 100)::numeric, 2)
            else 0
        end as utilization_rate,
        
        -- Dates
        issue_date,
        expiry_date,
        case 
            when expiry_date < current_date then true 
            else false 
        end as is_expired,
        
        date_part('year', age(current_date, issue_date))::int as card_age_years,
        
        -- Features
        contactless_enabled,
        online_enabled,
        international_enabled,
        is_business_card,
        
        last_updated,
        current_timestamp as dbt_created_at
        
    from mock_cards
    where card_status != 'INVALID'
),

enriched_cards as (
    select
        bc.*,
        
        -- Credit scoring
        case 
            when bc.utilization_rate > 90 then 'HIGH_UTILIZATION'
            when bc.utilization_rate > 70 then 'MEDIUM_UTILIZATION'
            when bc.utilization_rate > 30 then 'LOW_UTILIZATION'
            else 'MINIMAL_UTILIZATION'
        end as utilization_category,
        
        -- Available credit
        case 
            when bc.credit_limit > 0 then bc.credit_limit - bc.current_balance
            else null
        end as available_credit,
        
        -- Risk indicators
        case 
            when bc.utilization_rate > 85 then 90
            when bc.utilization_rate > 70 then 70
            when bc.utilization_rate > 50 then 50
            when bc.card_status = 'BLOCKED' then 95
            else 20
        end as risk_score,
        
        -- Digital features
        case 
            when bc.contactless_enabled and bc.online_enabled then 'FULL_DIGITAL'
            when bc.contactless_enabled or bc.online_enabled then 'PARTIAL_DIGITAL'
            else 'TRADITIONAL'
        end as digital_feature_level,
        
        -- Card tier (based on limits)
        case 
            when bc.credit_limit >= 50000 then 'PREMIUM'
            when bc.credit_limit >= 25000 then 'GOLD'
            when bc.credit_limit >= 10000 then 'SILVER'
            when bc.credit_limit > 0 then 'STANDARD'
            else 'N/A'
        end as card_tier,
        
        -- Status flags
        case 
            when bc.card_status = 'ACTIVE' and not bc.is_expired then true 
            else false 
        end as is_usable,
        
        case 
            when bc.card_age_years >= 5 then 'LONG_TERM'
            when bc.card_age_years >= 2 then 'ESTABLISHED'
            when bc.card_age_years >= 1 then 'MATURE'
            else 'NEW'
        end as tenure_category
        
    from base_cards bc
)

select * from enriched_cards 
{{ config(
    materialized='view',
    tags=['bronze', 'staging', 'transactions', 'financial']
) }}

/*
    Staging model for transaction history
    
    Captures all customer transactions across channels with
    enrichment for risk scoring and analytical purposes.
*/

with mock_transactions as (
    select
        series_value as transaction_id,
        'TXN' || lpad(series_value::text, 10, '0') as transaction_number,
        
        -- Link to accounts (transactions distributed across accounts)
        ((series_value - 1) % ({{ var('num_customers') }} * {{ var('num_accounts_multiplier') }})::int) + 1 as account_id,
        
        -- Transaction types
        case 
            when series_value % 10 = 1 then 'DEPOSIT'
            when series_value % 10 = 2 then 'WITHDRAWAL'
            when series_value % 10 = 3 then 'TRANSFER_IN'
            when series_value % 10 = 4 then 'TRANSFER_OUT'
            when series_value % 10 = 5 then 'PAYMENT'
            when series_value % 10 = 6 then 'PURCHASE'
            when series_value % 10 = 7 then 'FEE'
            when series_value % 10 = 8 then 'INTEREST'
            when series_value % 10 = 9 then 'DIVIDEND'
            else 'REFUND'
        end as transaction_type,
        
        -- Transaction amounts (realistic distribution)
        case 
            when series_value % 20 = 1 then (10000 + (series_value % 90000))::numeric(12,2)  -- Large amounts
            when series_value % 10 = 1 then (1000 + (series_value % 4000))::numeric(12,2)   -- Medium amounts
            when series_value % 5 = 1 then (100 + (series_value % 900))::numeric(12,2)      -- Small amounts
            else (10 + (series_value % 190))::numeric(12,2)                                  -- Micro amounts
        end as transaction_amount,
        
        -- Transaction status
        case 
            when series_value % 100 = 1 then 'FAILED'
            when series_value % 50 = 1 then 'PENDING'
            when series_value % 25 = 1 then 'CANCELLED'
            else 'COMPLETED'
        end as transaction_status,
        
        -- Channels
        case 
            when series_value % 6 = 1 then 'ATM'
            when series_value % 6 = 2 then 'ONLINE'
            when series_value % 6 = 3 then 'MOBILE'
            when series_value % 6 = 4 then 'BRANCH'
            when series_value % 6 = 5 then 'PHONE'
            else 'CARD'
        end as channel,
        
        -- Dates (distributed over last 2 years)
        '2023-01-01'::date + (series_value % 730) * interval '1 day' + 
        (series_value % 1440) * interval '1 minute' as transaction_datetime,
        
        -- Geographic data
        case 
            when series_value % 10 = 1 then 'New York, NY'
            when series_value % 10 = 2 then 'Los Angeles, CA'
            when series_value % 10 = 3 then 'Chicago, IL'
            when series_value % 10 = 4 then 'Houston, TX'
            when series_value % 10 = 5 then 'Phoenix, AZ'
            else 'Online'
        end as transaction_location,
        
        -- Merchant data for purchases
        case 
            when series_value % 10 = 6 then 
                case 
                    when series_value % 5 = 1 then 'Amazon.com'
                    when series_value % 5 = 2 then 'Walmart'
                    when series_value % 5 = 3 then 'Target'
                    when series_value % 5 = 4 then 'Starbucks'
                    else 'Gas Station'
                end
            else null
        end as merchant_name,
        
        -- Risk flags
        case when series_value % 100 = 1 then true else false end as is_high_risk,
        case when series_value % 50 = 1 then true else false end as requires_approval,
        
        current_timestamp as last_updated
        
    from generate_series(1, ({{ var('num_customers') }} * {{ var('num_accounts_multiplier') }} * 50)::int) as series_value
),

base_transactions as (
    select
        transaction_id,
        transaction_number,
        account_id,
        
        -- Transaction details
        upper(transaction_type) as transaction_type,
        transaction_amount,
        upper(transaction_status) as transaction_status,
        upper(channel) as channel,
        
        -- Timing
        transaction_datetime,
        transaction_datetime::date as transaction_date,
        extract(hour from transaction_datetime) as transaction_hour,
        extract(dow from transaction_datetime) as day_of_week,
        
        -- Location and merchant
        transaction_location,
        merchant_name,
        
        -- Risk indicators
        is_high_risk,
        requires_approval,
        
        -- Amount analysis
        case 
            when transaction_amount >= 10000 then 'LARGE'
            when transaction_amount >= 1000 then 'MEDIUM'
            when transaction_amount >= 100 then 'SMALL'
            else 'MICRO'
        end as amount_category,
        
        last_updated,
        current_timestamp as dbt_created_at
        
    from mock_transactions
    where transaction_status != 'INVALID'
),

categorized_transactions as (
    select
        bt.*,
        
        -- Channel analysis
        case 
            when bt.channel in ('ONLINE', 'MOBILE') then 'DIGITAL'
            when bt.channel in ('ATM', 'CARD') then 'SELF_SERVICE'
            when bt.channel in ('BRANCH', 'PHONE') then 'ASSISTED'
            else 'OTHER'
        end as channel_category,
        
        -- Time-based patterns
        case 
            when bt.transaction_hour between 9 and 17 then 'BUSINESS_HOURS'
            when bt.transaction_hour between 18 and 22 then 'EVENING'
            when bt.transaction_hour between 6 and 8 then 'MORNING'
            else 'OFF_HOURS'
        end as time_category,
        
        case 
            when bt.day_of_week in (1, 7) then 'WEEKEND'
            else 'WEEKDAY'
        end as day_category,
        
        -- Transaction flow
        case 
            when bt.transaction_type in ('DEPOSIT', 'TRANSFER_IN', 'INTEREST', 'DIVIDEND', 'REFUND') then 'INFLOW'
            when bt.transaction_type in ('WITHDRAWAL', 'TRANSFER_OUT', 'PAYMENT', 'PURCHASE', 'FEE') then 'OUTFLOW'
            else 'NEUTRAL'
        end as flow_direction,
        
        -- Digital adoption indicator
        case 
            when bt.channel in ('ONLINE', 'MOBILE') then true 
            else false 
        end as is_digital_transaction
        
    from base_transactions bt
),

enriched_transactions as (
    select
        ct.*,
        
        -- Risk scoring (now can reference time_category from previous CTE)
        case 
            when ct.is_high_risk then 90
            when ct.amount_category = 'LARGE' and ct.time_category = 'OFF_HOURS' then 75
            when ct.amount_category = 'LARGE' then 60
            when ct.time_category = 'OFF_HOURS' then 45
            else 20
        end as risk_score
        
    from categorized_transactions ct
)

select * from enriched_transactions 
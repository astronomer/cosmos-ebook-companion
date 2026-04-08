{{ config(
    materialized='view',
    tags=['bronze', 'staging', 'financial_products', 'accounts']
) }}

/*
    Staging model for customer accounts (checking, savings, investment)
    
    Standardizes account information across product types and
    enriches with product-specific business rules and risk indicators.
*/

with mock_accounts as (
    select
        series_value as account_id,
        'ACC' || lpad(series_value::text, 8, '0') as account_number,
        
        -- Link to customers (some customers have multiple accounts)
        case 
            when series_value <= {{ var('num_customers') }} then series_value  -- First account per customer
            when series_value <= ({{ var('num_customers') }} * 1.6)::int then (series_value % {{ var('num_customers') }}) + 1  -- Second accounts
            else (series_value % {{ var('num_customers') }}) + 1  -- Third accounts for active customers
        end as customer_id,
        
        -- Account product mapping
        case 
            when series_value % 6 = 1 then 'CHCK001'  -- Basic Checking
            when series_value % 6 = 2 then 'SAVE001'  -- High Yield Savings
            when series_value % 6 = 3 then 'CD001'    -- 12-Month CD
            when series_value % 6 = 4 then 'INVT001'  -- Brokerage Account
            when series_value % 6 = 5 then 'BUSI001'  -- Business Checking
            else 'CHCK001'  -- Default to checking
        end as product_id,
        
        -- Account balances (realistic distribution)
        case 
            when series_value % 10 = 1 then 100000 + (series_value % 500000)::numeric  -- High balance
            when series_value % 5 = 1 then 25000 + (series_value % 75000)::numeric    -- Medium balance
            else 500 + (series_value % 25000)::numeric  -- Standard balance
        end as current_balance,
        
        case 
            when series_value % 15 = 1 then 150000 + (series_value % 300000)::numeric  -- High average
            else 2000 + (series_value % 50000)::numeric  -- Standard average
        end as average_balance,
        
        -- Account status and dates
        case 
            when series_value % 50 = 1 then 'CLOSED'
            when series_value % 25 = 1 then 'DORMANT'
            when series_value % 20 = 1 then 'RESTRICTED'
            else 'ACTIVE'
        end as account_status,
        
        '2020-01-01'::date + (series_value * interval '2.5 days') as opened_date,
        case 
            when series_value % 50 = 1 then '2024-01-01'::date + (series_value * interval '1 day')
            else null
        end as closed_date,
        
        -- Interest and fees
        case 
            when series_value % 6 = 2 then 2.5  -- Savings rate
            when series_value % 6 = 3 then 4.25 -- CD rate
            when series_value % 6 = 5 then 0.5  -- Business rate
            else 0.01  -- Checking rate
        end as interest_rate,
        
        case 
            when series_value % 6 = 1 then 5.00   -- Checking fee
            when series_value % 6 = 5 then 15.00  -- Business fee
            else 0.00
        end as monthly_fee,
        
        -- Account features
        case when series_value % 3 != 1 then true else false end as online_banking_enabled,
        case when series_value % 4 != 1 then true else false end as mobile_banking_enabled,
        case when series_value % 5 != 1 then true else false end as overdraft_protection,
        case when series_value % 8 = 1 then true else false end as is_joint_account,
        
        current_timestamp as last_updated
        
    from generate_series(1, ({{ var('num_customers') }} * {{ var('num_accounts_multiplier') }})::int) as series_value
),

base_accounts as (
    select
        account_id,
        account_number,
        customer_id,
        product_id,
        
        -- Financial information
        current_balance,
        average_balance,
        interest_rate / 100.0 as annual_interest_rate,  -- Convert percentage to decimal
        monthly_fee,
        
        -- Account lifecycle
        upper(account_status) as account_status,
        opened_date,
        closed_date,
        case 
            when closed_date is null then true 
            else false 
        end as is_active,
        
        coalesce(
            date_part('year', age(coalesce(closed_date, current_date), opened_date))::int,
            0
        ) as account_age_years,
        
        -- Account features
        online_banking_enabled,
        mobile_banking_enabled,
        overdraft_protection,
        is_joint_account,
        
        last_updated,
        current_timestamp as dbt_created_at
        
    from mock_accounts
    where account_status != 'INVALID'
),

enriched_accounts as (
    select
        ba.*,
        
        -- Product enrichment
        pc.product_name,
        pc.product_type,
        pc.category as product_category,
        pc.min_balance as product_min_balance,
        pc.risk_level as product_risk_level,
        
        -- Balance analysis
        case 
            when ba.current_balance >= 100000 then 'High Balance'
            when ba.current_balance >= 25000 then 'Medium Balance'
            when ba.current_balance >= 5000 then 'Standard Balance'
            else 'Low Balance'
        end as balance_tier,
        
        case 
            when ba.current_balance < pc.min_balance then true 
            else false 
        end as below_minimum_balance,
        
        -- Revenue calculations
        (ba.average_balance * ba.annual_interest_rate / 12) as monthly_interest_expense,
        ba.monthly_fee as monthly_fee_revenue,
        (ba.monthly_fee - (ba.average_balance * ba.annual_interest_rate / 12)) as monthly_net_revenue,
        
        -- Account relationship indicators
        case 
            when ba.account_age_years >= 10 then 'Long Term'
            when ba.account_age_years >= 5 then 'Established'
            when ba.account_age_years >= 2 then 'Mature'
            else 'New'
        end as relationship_tenure,
        
        -- Digital adoption
        case 
            when ba.online_banking_enabled and ba.mobile_banking_enabled then 'Full Digital'
            when ba.online_banking_enabled or ba.mobile_banking_enabled then 'Partial Digital'
            else 'Traditional'
        end as digital_adoption_level,
        
        -- Risk indicators
        case 
            when ba.current_balance <= 0 then 'Negative Balance'
            when ba.current_balance < pc.min_balance then 'Below Minimum'
            when ba.account_status = 'RESTRICTED' then 'Restricted'
            when ba.account_status = 'DORMANT' then 'Dormant'
            else 'Normal'
        end as risk_status,
        
        -- Service level indicators
        case 
            when ba.current_balance >= 250000 then 'Private Banking'
            when ba.current_balance >= 100000 then 'Premium'
            when ba.current_balance >= 25000 then 'Preferred'
            else 'Standard'
        end as service_tier,
        
        -- Fee waiver eligibility
        case 
            when ba.current_balance >= pc.min_balance and ba.monthly_fee > 0 then true 
            else false 
        end as fee_waiver_eligible,
        
        -- Account scoring for cross-sell
        case 
            when ba.current_balance >= 50000 and ba.account_age_years >= 2 then 90
            when ba.current_balance >= 25000 and ba.account_age_years >= 1 then 75
            when ba.current_balance >= 10000 then 60
            when ba.account_status = 'ACTIVE' then 45
            else 20
        end as cross_sell_score
        
    from base_accounts ba
    left join {{ ref('product_catalog') }} pc on ba.product_id = pc.product_id
)

select * from enriched_accounts 
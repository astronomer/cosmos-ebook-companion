{{ config(
    materialized='view',
    tags=['bronze', 'staging', 'loans', 'credit']
) }}

/*
    Staging model for loan products
    
    Covers mortgages, personal loans, auto loans with
    payment history and risk assessment metrics.
*/

with mock_loans as (
    select
        series_value as loan_id,
        'LOAN' || lpad(series_value::text, 8, '0') as loan_number,
        
        -- Link to customers (subset of customers have loans)
        ((series_value - 1) % {{ var('num_customers') }}) + 1 as customer_id,
        
        -- Loan types
        case 
            when series_value % 4 = 1 then 'MORTGAGE'
            when series_value % 4 = 2 then 'PERSONAL'
            when series_value % 4 = 3 then 'AUTO'
            else 'HOME_EQUITY'
        end as loan_type,
        
        -- Loan amounts based on type
        case 
            when series_value % 4 = 1 then (250000 + (series_value % 750000))::numeric(12,2)  -- Mortgage
            when series_value % 4 = 2 then (5000 + (series_value % 45000))::numeric(12,2)    -- Personal
            when series_value % 4 = 3 then (15000 + (series_value % 70000))::numeric(12,2)   -- Auto
            else (50000 + (series_value % 200000))::numeric(12,2)                             -- Home equity
        end as original_amount,
        
        -- Current balances (reduced from original)
        case 
            when series_value % 4 = 1 then (200000 + (series_value % 600000))::numeric(12,2)  -- Mortgage
            when series_value % 4 = 2 then (2000 + (series_value % 30000))::numeric(12,2)     -- Personal
            when series_value % 4 = 3 then (8000 + (series_value % 50000))::numeric(12,2)     -- Auto
            else (30000 + (series_value % 150000))::numeric(12,2)                              -- Home equity
        end as current_balance,
        
        -- Interest rates based on type
        case 
            when series_value % 4 = 1 then 3.5 + (series_value % 30) / 10.0  -- Mortgage: 3.5-6.4%
            when series_value % 4 = 2 then 8.0 + (series_value % 80) / 10.0  -- Personal: 8.0-15.9%
            when series_value % 4 = 3 then 4.0 + (series_value % 60) / 10.0  -- Auto: 4.0-9.9%
            else 5.0 + (series_value % 50) / 10.0                            -- Home equity: 5.0-9.9%
        end as interest_rate,
        
        -- Loan terms (months)
        case 
            when series_value % 4 = 1 then 360  -- Mortgage: 30 years
            when series_value % 4 = 2 then 60   -- Personal: 5 years
            when series_value % 4 = 3 then 72   -- Auto: 6 years
            else 180                             -- Home equity: 15 years
        end as term_months,
        
        -- Loan status
        case 
            when series_value % 50 = 1 then 'DELINQUENT'
            when series_value % 30 = 1 then 'DEFAULT'
            when series_value % 20 = 1 then 'PAID_OFF'
            when series_value % 15 = 1 then 'CHARGED_OFF'
            else 'CURRENT'
        end as loan_status,
        
        -- Dates
        '2018-01-01'::date + (series_value % 2190) * interval '1 day' as origination_date,
        case 
            when series_value % 20 = 1 then '2018-01-01'::date + (series_value % 2190) * interval '1 day' + interval '3 years'
            else null
        end as maturity_date,
        
        -- Payment info
        case 
            when series_value % 10 = 1 then (series_value % 30) + 1  -- Days past due
            else 0
        end as days_past_due,
        
        case when series_value % 5 = 1 then true else false end as has_collateral,
        case when series_value % 8 = 1 then true else false end as is_secured,
        
        current_timestamp as last_updated
        
    from generate_series(1, ({{ var('num_customers') }} * 0.6)::int) as series_value
),

base_loans as (
    select
        loan_id,
        loan_number,
        customer_id,
        
        -- Loan details
        upper(loan_type) as loan_type,
        original_amount,
        current_balance,
        original_amount - current_balance as principal_paid,
        
        -- Terms
        round(interest_rate::numeric, 2) as interest_rate,
        term_months,
        
        -- Status
        upper(loan_status) as loan_status,
        days_past_due,
        
        -- Dates
        origination_date,
        maturity_date,
        date_part('year', age(current_date, origination_date))::int as loan_age_years,
        
        -- Security
        has_collateral,
        is_secured,
        
        -- Calculated fields
        case 
            when current_balance > 0 and original_amount > 0 then 
                round(((original_amount - current_balance) / original_amount * 100)::numeric, 2)
            else 100
        end as percent_paid,
        
        last_updated,
        current_timestamp as dbt_created_at
        
    from mock_loans
    where loan_status != 'INVALID'
),

enriched_loans as (
    select
        bl.*,
        
        -- Payment status categories
        case 
            when bl.days_past_due = 0 then 'CURRENT'
            when bl.days_past_due <= 30 then 'LATE_1_30'
            when bl.days_past_due <= 60 then 'LATE_31_60'
            when bl.days_past_due <= 90 then 'LATE_61_90'
            else 'LATE_90_PLUS'
        end as delinquency_bucket,
        
        -- Risk scoring
        case 
            when bl.loan_status = 'CHARGED_OFF' then 100
            when bl.loan_status = 'DEFAULT' then 95
            when bl.days_past_due > 90 then 85
            when bl.days_past_due > 60 then 70
            when bl.days_past_due > 30 then 55
            when bl.days_past_due > 0 then 40
            else 20
        end as risk_score,
        
        -- Loan performance
        case 
            when bl.percent_paid >= 80 then 'EXCELLENT'
            when bl.percent_paid >= 60 then 'GOOD'
            when bl.percent_paid >= 40 then 'FAIR'
            when bl.percent_paid >= 20 then 'POOR'
            else 'NEW'
        end as payment_performance,
        
        -- Loan size categorization
        case 
            when bl.original_amount >= 500000 then 'JUMBO'
            when bl.original_amount >= 200000 then 'LARGE'
            when bl.original_amount >= 50000 then 'MEDIUM'
            when bl.original_amount >= 10000 then 'SMALL'
            else 'MICRO'
        end as loan_size_category,
        
        -- Rate competitiveness
        case 
            when bl.loan_type = 'MORTGAGE' and bl.interest_rate < 4.0 then 'EXCELLENT_RATE'
            when bl.loan_type = 'MORTGAGE' and bl.interest_rate < 5.0 then 'GOOD_RATE'
            when bl.loan_type = 'PERSONAL' and bl.interest_rate < 10.0 then 'EXCELLENT_RATE'
            when bl.loan_type = 'PERSONAL' and bl.interest_rate < 12.0 then 'GOOD_RATE'
            when bl.loan_type = 'AUTO' and bl.interest_rate < 5.0 then 'EXCELLENT_RATE'
            when bl.loan_type = 'AUTO' and bl.interest_rate < 7.0 then 'GOOD_RATE'
            else 'MARKET_RATE'
        end as rate_competitiveness,
        
        -- Remaining term estimation
        case 
            when bl.maturity_date is not null then 
                date_part('month', age(bl.maturity_date, current_date))::int
            else bl.term_months - (bl.loan_age_years * 12)
        end as estimated_months_remaining,
        
        -- Flags
        case 
            when bl.loan_status in ('CURRENT', 'PAID_OFF') and bl.days_past_due = 0 then true 
            else false 
        end as is_performing,
        
        case 
            when bl.loan_age_years >= 10 then 'LONG_TERM'
            when bl.loan_age_years >= 5 then 'ESTABLISHED'
            when bl.loan_age_years >= 2 then 'MATURE'
            else 'NEW'
        end as tenure_category
        
    from base_loans bl
)

select * from enriched_loans 
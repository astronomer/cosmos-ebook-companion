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
        ROW_NUMBER() OVER (ORDER BY NULL) as loan_id,
        ((ROW_NUMBER() OVER (ORDER BY NULL) - 1) % {{ var('num_customers') }}) + 1 as customer_id,
        
        case 
            when ROW_NUMBER() OVER (ORDER BY NULL) % 4 = 1 then 'PERSONAL'
            when ROW_NUMBER() OVER (ORDER BY NULL) % 4 = 2 then 'AUTO'
            when ROW_NUMBER() OVER (ORDER BY NULL) % 4 = 3 then 'MORTGAGE'
            else 'STUDENT'
        end as loan_type,
        
        DATEADD(day, -(ROW_NUMBER() OVER (ORDER BY NULL) % 1095), '2024-01-01'::date) as loan_date,
        
        case 
            when ROW_NUMBER() OVER (ORDER BY NULL) % 4 = 1 then (5000 + (ROW_NUMBER() OVER (ORDER BY NULL) % 45000))::numeric(12,2)
            when ROW_NUMBER() OVER (ORDER BY NULL) % 4 = 2 then (15000 + (ROW_NUMBER() OVER (ORDER BY NULL) % 50000))::numeric(12,2)
            when ROW_NUMBER() OVER (ORDER BY NULL) % 4 = 3 then (200000 + (ROW_NUMBER() OVER (ORDER BY NULL) % 500000))::numeric(12,2)
            else (10000 + (ROW_NUMBER() OVER (ORDER BY NULL) % 40000))::numeric(12,2)
        end as loan_amount,
        
        case 
            when ROW_NUMBER() OVER (ORDER BY NULL) % 15 = 1 then 'DEFAULTED'
            when ROW_NUMBER() OVER (ORDER BY NULL) % 10 = 1 then 'PAID_OFF'
            else 'ACTIVE'
        end as loan_status,
        
        current_timestamp as last_updated
        
    from TABLE(GENERATOR(ROWCOUNT => ({{ var('num_customers') }} * 2)::int))
),

base_loans as (
    select
        loan_id,
        customer_id,
        
        -- Loan details
        UPPER(loan_type) as loan_type,
        loan_amount,
        
        -- Loan status
        UPPER(loan_status) as loan_status,
        
        -- Dates
        loan_date,
        DATEDIFF(year, loan_date, current_date)::int as loan_age_years,
        
        -- Payment info
        case 
            when loan_status = 'PAID_OFF' then 100
            else 0
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
            when bl.loan_age_years = 0 then 'CURRENT'
            when bl.loan_age_years <= 3 then 'NEW'
            when bl.loan_age_years <= 6 then 'ESTABLISHED'
            when bl.loan_age_years <= 9 then 'MATURE'
            else 'LONG_TERM'
        end as tenure_category,
        
        -- Risk scoring
        case 
            when bl.loan_status = 'DEFAULTED' then 100
            when bl.loan_status = 'DEFAULT' then 95
            when bl.loan_age_years > 9 then 85
            when bl.loan_age_years > 6 then 70
            when bl.loan_age_years > 3 then 55
            else 40
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
            when bl.loan_amount >= 500000 then 'JUMBO'
            when bl.loan_amount >= 200000 then 'LARGE'
            when bl.loan_amount >= 50000 then 'MEDIUM'
            when bl.loan_amount >= 10000 then 'SMALL'
            else 'MICRO'
        end as loan_size_category,
        
        -- Rate competitiveness
        case 
            when bl.loan_type = 'MORTGAGE' and bl.loan_amount < 200000 then 'EXCELLENT_RATE'
            when bl.loan_type = 'MORTGAGE' and bl.loan_amount < 500000 then 'GOOD_RATE'
            when bl.loan_type = 'PERSONAL' and bl.loan_amount < 100000 then 'EXCELLENT_RATE'
            when bl.loan_type = 'PERSONAL' and bl.loan_amount < 150000 then 'GOOD_RATE'
            when bl.loan_type = 'AUTO' and bl.loan_amount < 150000 then 'EXCELLENT_RATE'
            when bl.loan_type = 'AUTO' and bl.loan_amount < 200000 then 'GOOD_RATE'
            else 'MARKET_RATE'
        end as rate_competitiveness,
        
        -- Remaining term estimation
        case 
            when bl.loan_age_years >= 10 then 120
            when bl.loan_age_years >= 5 then 60
            else 12
        end as estimated_months_remaining,
        
        -- Flags
        case 
            when bl.loan_status = 'PAID_OFF' then true 
            else false 
        end as is_performing
        
    from base_loans bl
)

select * from enriched_loans 
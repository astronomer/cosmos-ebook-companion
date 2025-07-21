{{ config(
    materialized='view',
    tags=['bronze', 'staging', 'customers']
) }}

/*
    Staging model for customer master data
    
    This model creates a clean, standardized view of customer information
    from multiple source systems, applying basic data quality rules and
    enriching with reference data lookups.
*/

with mock_customers as (
    -- Generate realistic customer data for demo
    select
        -- Customer identifiers
        ROW_NUMBER() OVER (ORDER BY NULL) as customer_id,
        'CUST' || LPAD(ROW_NUMBER() OVER (ORDER BY NULL)::string, 6, '0') as customer_number,
        
        -- Personal information
        case 
            when ROW_NUMBER() OVER (ORDER BY NULL) % 4 = 1 then 'John'
            when ROW_NUMBER() OVER (ORDER BY NULL) % 4 = 2 then 'Jane'
            when ROW_NUMBER() OVER (ORDER BY NULL) % 4 = 3 then 'Michael'
            else 'Sarah'
        end as first_name,
        
        case 
            when ROW_NUMBER() OVER (ORDER BY NULL) % 5 = 1 then 'Smith'
            when ROW_NUMBER() OVER (ORDER BY NULL) % 5 = 2 then 'Johnson'
            when ROW_NUMBER() OVER (ORDER BY NULL) % 5 = 3 then 'Williams'
            when ROW_NUMBER() OVER (ORDER BY NULL) % 5 = 4 then 'Brown'
            else 'Davis'
        end as last_name,
        
        -- Demographics
        DATEADD(day, ROW_NUMBER() OVER (ORDER BY NULL) * 45, '1980-01-01'::date) as date_of_birth,
        case when ROW_NUMBER() OVER (ORDER BY NULL) % 2 = 1 then 'M' else 'F' end as gender,
        
        -- Contact information
        'customer' || ROW_NUMBER() OVER (ORDER BY NULL) || '@email.com' as email_address,
        case 
            when ROW_NUMBER() OVER (ORDER BY NULL) % 10 = 1 then '555-01' || LPAD((ROW_NUMBER() OVER (ORDER BY NULL) % 100)::string, 2, '0') || '-' || LPAD((ROW_NUMBER() OVER (ORDER BY NULL) % 10000)::string, 4, '0')
            else '555-02' || LPAD((ROW_NUMBER() OVER (ORDER BY NULL) % 100)::string, 2, '0') || '-' || LPAD((ROW_NUMBER() OVER (ORDER BY NULL) % 10000)::string, 4, '0')
        end as phone_number,
        
        -- Financial information
        case 
            when ROW_NUMBER() OVER (ORDER BY NULL) % 10 = 1 then 150000 + (ROW_NUMBER() OVER (ORDER BY NULL) % 300000)
            when ROW_NUMBER() OVER (ORDER BY NULL) % 10 = 2 then 75000 + (ROW_NUMBER() OVER (ORDER BY NULL) % 75000)
            else 45000 + (ROW_NUMBER() OVER (ORDER BY NULL) % 100000)
        end as annual_income,
        
        -- Credit information
        case 
            when ROW_NUMBER() OVER (ORDER BY NULL) % 20 = 1 then 300 + (ROW_NUMBER() OVER (ORDER BY NULL) % 200)  -- Poor credit
            when ROW_NUMBER() OVER (ORDER BY NULL) % 10 = 1 then 720 + (ROW_NUMBER() OVER (ORDER BY NULL) % 130)  -- Excellent credit
            else 580 + (ROW_NUMBER() OVER (ORDER BY NULL) % 140)  -- Fair to good credit
        end as credit_score,
        
        -- Account status
        case 
            when ROW_NUMBER() OVER (ORDER BY NULL) % 50 = 1 then 'CLOSED'
            when ROW_NUMBER() OVER (ORDER BY NULL) % 25 = 1 then 'SUSPENDED'
            else 'ACTIVE'
        end as customer_status,
        
        -- Dates
        DATEADD(day, ROW_NUMBER() OVER (ORDER BY NULL) * 2, '2020-01-01'::date) as customer_since_date,
        current_timestamp as last_updated,
        current_timestamp as dbt_created_at
        
    from TABLE(GENERATOR(ROWCOUNT => {{ var('num_customers') }}))
),

base_customers as (
    select
        -- Customer identifiers
        customer_id,
        customer_number,
        
        -- Personal information
        INITCAP(first_name) as first_name,
        INITCAP(last_name) as last_name,
        CONCAT(INITCAP(first_name), ' ', INITCAP(last_name)) as full_name,
        
        -- Demographics
        date_of_birth,
        DATEDIFF(year, date_of_birth, current_date) as age,
        case 
            when DATEDIFF(year, date_of_birth, current_date) < 25 then 'AC010'  -- Young Adult
            when DATEDIFF(year, date_of_birth, current_date) < 35 then 'AC006'  -- Young Professional  
            when DATEDIFF(year, date_of_birth, current_date) < 50 then 'AC007'  -- Mid-Career
            when DATEDIFF(year, date_of_birth, current_date) < 65 then 'AC008'  -- Pre-Retirement
            else 'AC009'  -- Retired
        end as age_cohort_id,
        
        UPPER(gender) as gender,
        
        -- Contact information
        LOWER(email_address) as email_address,
        phone_number,
        
        -- Financial classification
        annual_income,
        case 
            when annual_income <= 25000 then 'IB001'      -- Low Income
            when annual_income <= 50000 then 'IB002'      -- Lower Middle
            when annual_income <= 75000 then 'IB003'      -- Middle Income
            when annual_income <= 100000 then 'IB004'     -- Upper Middle
            when annual_income <= 150000 then 'IB005'     -- High Income
            when annual_income <= 250000 then 'IB006'     -- Affluent
            else 'IB007'                                   -- High Net Worth
        end as income_bracket_id,
        
        -- Credit assessment
        credit_score,
        case 
            when credit_score >= 800 then 'CSR001'        -- Exceptional
            when credit_score >= 740 then 'CSR002'        -- Very Good
            when credit_score >= 670 then 'CSR003'        -- Good
            when credit_score >= 580 then 'CSR004'        -- Fair
            when credit_score >= 500 then 'CSR005'        -- Poor
            else 'CSR006'                                  -- Very Poor
        end as credit_score_range_id,
        
        -- Risk classification
        case 
            when credit_score >= 740 and annual_income >= 75000 then 'RC002'  -- Low Risk
            when credit_score >= 670 and annual_income >= 50000 then 'RC003'  -- Moderate Risk
            when credit_score >= 580 then 'RC004'                             -- Elevated Risk
            when credit_score >= 500 then 'RC005'                             -- High Risk
            else 'RC006'                                                       -- Severe Risk
        end as risk_category_id,
        
        -- Account information
        UPPER(customer_status) as customer_status,
        customer_since_date,
        DATEDIFF(year, customer_since_date, current_date) as relationship_years,
        case 
            when DATEDIFF(month, customer_since_date, current_date) <= 2 then 'LS003'  -- New Customer
            when DATEDIFF(year, customer_since_date, current_date) < 1 then 'LS004'    -- Growing
            when DATEDIFF(year, customer_since_date, current_date) < 3 then 'LS005'    -- Established
            when DATEDIFF(year, customer_since_date, current_date) < 5 then 'LS006'    -- Mature
            else 'LS007'                                                                        -- Loyal
        end as lifecycle_stage_id,
        
        -- Metadata
        last_updated,
        dbt_created_at
        
    from mock_customers
    where customer_status != 'INVALID'  -- Basic data quality filter
),

enriched_customers as (
    select
        bc.*,
        
        -- Enrichment from reference data
        ac.cohort_name as age_cohort_name,
        ac.characteristics as age_cohort_characteristics,
        
        ib.bracket_name as income_bracket_name,
        ib.marketing_priority as income_marketing_priority,
        
        csr.range_name as credit_score_range_name,
        csr.grade as credit_grade,
        csr.default_rate as expected_default_rate,
        
        rc.risk_level as risk_level,
        rc.risk_name as risk_name,
        rc.monitoring_frequency as risk_monitoring_frequency,
        
        ls.stage_name as lifecycle_stage_name,
        ls.characteristics as lifecycle_characteristics,
        
        -- Customer value indicators
        case 
            when bc.annual_income >= 250000 and bc.credit_score >= 740 then 'High Value'
            when bc.annual_income >= 100000 and bc.credit_score >= 670 then 'Medium Value'
            else 'Standard Value'
        end as customer_value_segment,
        
        -- Marketing eligibility flags
        case when bc.age >= 18 and bc.customer_status = 'ACTIVE' then true else false end as marketing_eligible,
        case when bc.credit_score >= 580 then true else false end as lending_eligible,
        case when bc.annual_income >= 50000 then true else false end as premium_product_eligible
        
    from base_customers bc
    left join {{ ref('age_cohorts') }} ac on bc.age_cohort_id = ac.cohort_id
    left join {{ ref('income_brackets') }} ib on bc.income_bracket_id = ib.bracket_id
    left join {{ ref('credit_score_ranges') }} csr on bc.credit_score_range_id = csr.score_range_id
    left join {{ ref('risk_categories') }} rc on bc.risk_category_id = rc.risk_category_id
    left join {{ ref('lifecycle_stages') }} ls on bc.lifecycle_stage_id = ls.stage_id
)

select * from enriched_customers 
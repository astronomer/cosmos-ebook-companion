{{ config(
    materialized='view',
    tags=['silver', 'intermediate', 'financial_metrics', 'dashboard']
) }}

/*
    Account Performance Dashboard
    
    Executive-level account performance metrics combining multiple intermediate models:
    - Transaction analytics (int_transaction_analytics)
    - Customer financial summary (int_customer_financial_summary)
    - Customer profile (int_customer_profile)
    - Risk profile (int_comprehensive_risk_profile)
    - Product penetration analysis (int_product_penetration_analysis)
    - Account and balance data (stg_accounts, stg_account_balances)
    
    Creates the ultimate dependency cascade for beautiful DAG visualization!
    This is the CROWN JEWEL of intermediate models - depends on 5 other intermediate models!
*/

with accounts_base as (
    select
        account_id,
        customer_id,
        account_number,
        product_type,
        account_status,
        current_balance,
        annual_interest_rate,
        monthly_fee,
        opened_date,
        DATEDIFF(day, opened_date, current_date) as days_since_opened,
        DATEDIFF(year, opened_date, current_date) as account_age_years
    from {{ ref('stg_accounts') }}
),

balance_ranked as (
    select
        account_id,
        balance_amount as current_balance,
        available_balance,
        balance_date as last_balance_date,
        lag(balance_amount) over (partition by account_id order by balance_date) as previous_balance,
        balance_amount - lag(balance_amount) over (partition by account_id order by balance_date) as balance_change,
        row_number() over (partition by account_id order by balance_date desc) as rn
    from {{ ref('stg_account_balances') }}
),

latest_balances as (
    select
        account_id,
        current_balance,
        available_balance,
        last_balance_date,
        previous_balance,
        balance_change
    from balance_ranked
    where rn = 1
),

transaction_performance as (
    select
        account_id,
        total_transactions,
        total_transaction_volume,
        avg_transaction_amount,
        largest_transaction,
        net_cash_flow,
        total_deposits,
        total_deposit_amount,
        total_withdrawals,
        total_withdrawal_amount,
        total_payments,
        total_payment_amount,
        total_fees,
        total_fee_amount,
        digital_transaction_percentage,
        fee_to_volume_ratio,
        spending_category,
        activity_level,
        last_transaction_date
    from {{ ref('int_transaction_analytics') }}
),

customer_financial_profile as (
    select
        customer_id,
        total_deposit_accounts,
        total_deposit_balances,
        total_cards,
        total_credit_limit,
        total_loans,
        total_loan_balance,
        total_investments,
        total_investment_value,
        net_worth_with_bank,
        total_relationship_value,
        product_penetration_score,
        customer_risk_profile as financial_risk_level,  -- Updated column name
        customer_value_segment as wealth_tier  -- Updated column name
    from {{ ref('int_customer_financial_summary') }}
),

customer_profile_data as (
    select
        customer_id,
        full_name,
        age,
        relationship_years,
        lifecycle_stage_name,
        customer_value_segment,
        engagement_level,
        at_risk_customer,
        digital_preference,
        avg_satisfaction_score
    from {{ ref('int_customer_profile') }}
),

risk_assessment as (
    select
        customer_id,
        composite_risk_rating as overall_risk_level,  -- Updated column name
        compliance_status,
        composite_risk_score,
        current_credit_risk_score,
        current_fraud_risk_score,
        total_fraud_alerts,
        case when overall_risk_assessment = 'REQUIRES_MONITORING' then true else false end as requires_enhanced_monitoring,  -- Derived
        case when overall_risk_assessment = 'REQUIRES_IMMEDIATE_REVIEW' then true else false end as requires_immediate_review  -- Derived
    from {{ ref('int_comprehensive_risk_profile') }}
),

product_analytics as (
    select
        customer_id,
        product_penetration_score as product_usage_score,
        digital_adoption_level,
        marketing_responsiveness,
        channel_preference,
        cross_sell_opportunity_score,
        product_stickiness_score,
        next_best_product
    from {{ ref('int_product_penetration_analysis') }}
)

select
    -- Account Identifiers
    ab.account_id,
    ab.customer_id,
    ab.account_number,
    ab.product_type,
    ab.account_status,
    
    -- Customer Information
    cpd.full_name as customer_name,
    cpd.age as customer_age,
    cpd.relationship_years,
    cpd.lifecycle_stage_name,
    cpd.customer_value_segment,
    
    -- Account Basics
    ab.opened_date,
    ab.account_age_years,
    ab.annual_interest_rate,
    ab.monthly_fee,
    
    -- Balance Information
    coalesce(lb.current_balance, ab.current_balance) as current_balance,
    lb.available_balance,
    lb.previous_balance,
    lb.balance_change,
    lb.last_balance_date,
    
    -- Transaction Performance
    coalesce(tp.total_transactions, 0) as total_transactions,
    coalesce(tp.total_transaction_volume, 0) as total_transaction_volume,
    coalesce(tp.net_cash_flow, 0) as net_cash_flow,
    coalesce(tp.total_deposit_amount, 0) as total_deposit_amount,
    coalesce(tp.total_payment_amount, 0) as total_payment_amount,
    coalesce(tp.total_fee_amount, 0) as total_fees_paid,
    coalesce(tp.digital_transaction_percentage, 0) as digital_transaction_percentage,
    tp.spending_category,
    tp.activity_level,
    tp.last_transaction_date,
    
    -- Customer Financial Profile
    coalesce(cfp.total_deposit_accounts, 0) as customer_total_accounts,
    coalesce(cfp.total_deposit_balances, 0) as customer_total_balances,
    coalesce(cfp.total_relationship_value, 0) as customer_relationship_value,
    coalesce(cfp.product_penetration_score, 0) as customer_product_penetration,
    cfp.financial_risk_level as customer_financial_risk,
    cfp.wealth_tier as customer_wealth_tier,
    
    -- Digital & Engagement
    cpd.engagement_level,
    cpd.digital_preference,
    cpd.avg_satisfaction_score,
    coalesce(pa.digital_adoption_level, 'UNKNOWN') as digital_adoption_level,
    coalesce(pa.marketing_responsiveness, 'UNKNOWN') as marketing_responsiveness,
    coalesce(pa.channel_preference, 'UNKNOWN') as channel_preference,
    
    -- Risk Assessment
    coalesce(ra.overall_risk_level, 'UNKNOWN') as overall_risk_level,
    coalesce(ra.compliance_status, 'UNKNOWN') as compliance_status,
    coalesce(ra.composite_risk_score, 500) as composite_risk_score,
    coalesce(ra.total_fraud_alerts, 0) as total_fraud_alerts,
    coalesce(ra.requires_enhanced_monitoring, false) as requires_enhanced_monitoring,
    coalesce(ra.requires_immediate_review, false) as requires_immediate_review,
    
    -- Product & Cross-Sell
    coalesce(pa.cross_sell_opportunity_score, 0) as cross_sell_opportunity_score,
    coalesce(pa.product_stickiness_score, 0) as product_stickiness_score,
    pa.next_best_product,
    
    -- Account Performance Metrics
    
    -- Account Profitability Score (0-100)
    least(100, greatest(0,
        -- Fee revenue component
        (case when ab.monthly_fee > 0 then 20 else 0 end) +
        -- Transaction volume component
        (case 
            when tp.total_transaction_volume >= 50000 then 25
            when tp.total_transaction_volume >= 20000 then 20
            when tp.total_transaction_volume >= 10000 then 15
            when tp.total_transaction_volume >= 5000 then 10
            else 5
        end) +
        -- Balance component
        (case 
            when coalesce(lb.current_balance, ab.current_balance) >= 100000 then 25
            when coalesce(lb.current_balance, ab.current_balance) >= 50000 then 20
            when coalesce(lb.current_balance, ab.current_balance) >= 25000 then 15
            when coalesce(lb.current_balance, ab.current_balance) >= 10000 then 10
            else 5
        end) +
        -- Activity component
        (case 
            when tp.activity_level = 'VERY_ACTIVE' then 15
            when tp.activity_level = 'ACTIVE' then 12
            when tp.activity_level = 'MODERATE' then 8
            when tp.activity_level = 'LIGHT' then 5
            else 0
        end) +
        -- Digital adoption bonus
        (case when tp.digital_transaction_percentage > 70 then 10 else 0 end) +
        -- Longevity bonus
        (case when ab.account_age_years >= 5 then 5 else 0 end)
    )) as account_profitability_score,
    
    -- Account Health Score (0-100)
    least(100, greatest(0,
        -- Balance health (40 points max)
        (case 
            when coalesce(lb.current_balance, ab.current_balance) >= 10000 then 40
            when coalesce(lb.current_balance, ab.current_balance) >= 5000 then 30
            when coalesce(lb.current_balance, ab.current_balance) >= 1000 then 20
            when coalesce(lb.current_balance, ab.current_balance) >= 0 then 10
            else 0
        end) +
        -- Activity health (20 points max)
        (case 
            when tp.total_transactions >= 50 then 20
            when tp.total_transactions >= 20 then 15
            when tp.total_transactions >= 10 then 10
            when tp.total_transactions >= 1 then 5
            else 0
        end) +
        -- Risk health (20 points max)
        (case 
            when ra.overall_risk_level = 'LOW_RISK' then 20
            when ra.overall_risk_level = 'MEDIUM_RISK' then 10
            when ra.overall_risk_level = 'HIGH_RISK' then 5
            else 0
        end) +
        -- Customer satisfaction (10 points max)
        (case 
            when cpd.avg_satisfaction_score >= 4 then 10
            when cpd.avg_satisfaction_score >= 3 then 7
            when cpd.avg_satisfaction_score >= 2 then 4
            else 0
        end) +
        -- Compliance health (10 points max)
        (case 
            when ra.compliance_status = 'COMPLIANT' then 10
            when ra.compliance_status = 'MINOR_CONCERNS' then 5
            else 0
        end)
    )) as account_health_score,
    
    -- Account Risk Flags
    case 
        when ra.requires_immediate_review then 'IMMEDIATE_REVIEW_REQUIRED'
        when ra.requires_enhanced_monitoring then 'ENHANCED_MONITORING'
        when cpd.at_risk_customer then 'CUSTOMER_AT_RISK'
        when tp.total_transactions = 0 and ab.account_age_years > 1 then 'DORMANT_ACCOUNT'
        when coalesce(lb.current_balance, ab.current_balance) <= 0 then 'NEGATIVE_BALANCE'
        else 'NORMAL'
    end as account_risk_flag,
    
    -- Account Opportunity Classification
    case 
        when pa.cross_sell_opportunity_score >= 75 then 'HIGH_CROSS_SELL_OPPORTUNITY'
        when cfp.wealth_tier in ('HIGH_VALUE', 'MEDIUM_VALUE') then 'WEALTH_MANAGEMENT_OPPORTUNITY'
        when tp.digital_transaction_percentage < 30 and cpd.age < 50 then 'DIGITAL_CONVERSION_OPPORTUNITY'
        when tp.activity_level = 'MINIMAL' and ab.account_age_years < 2 then 'ACTIVATION_OPPORTUNITY'
        when cfp.product_penetration_score < 40 then 'PRODUCT_EXPANSION_OPPORTUNITY'
        else 'MAINTAIN_RELATIONSHIP'
    end as account_opportunity,
    
    -- Next Best Action for Account
    case 
        when ra.requires_immediate_review then 'RISK_REVIEW'
        when cpd.at_risk_customer and cpd.avg_satisfaction_score < 3 then 'RETENTION_CALL'
        when tp.total_transactions = 0 and ab.account_age_years > 1 then 'DORMANT_REACTIVATION'
        when pa.cross_sell_opportunity_score >= 75 then 'CROSS_SELL_CAMPAIGN'
        when tp.digital_transaction_percentage < 30 and cpd.age < 50 then 'DIGITAL_ONBOARDING'
        when coalesce(lb.current_balance, ab.current_balance) >= 50000 and cfp.total_investments = 0 then 'INVESTMENT_CONSULTATION'
        else 'STANDARD_MAINTENANCE'
    end as next_best_action,
    
    -- Account Tier for Servicing
    case 
        when cfp.wealth_tier = 'HIGH_VALUE' then 'PRIVATE_BANKING'
        when cfp.wealth_tier = 'MEDIUM_VALUE' then 'WEALTH_MANAGEMENT'
        when coalesce(lb.current_balance, ab.current_balance) >= 100000 then 'PREMIUM'
        when cfp.product_penetration_score >= 60 then 'PREFERRED'
        when tp.activity_level in ('VERY_ACTIVE', 'ACTIVE') then 'ACTIVE'
        else 'STANDARD'
    end as account_service_tier,
    
    current_timestamp as last_updated

from accounts_base ab
left join latest_balances lb on ab.account_id = lb.account_id
left join transaction_performance tp on ab.account_id = tp.account_id
left join customer_financial_profile cfp on ab.customer_id = cfp.customer_id
left join customer_profile_data cpd on ab.customer_id = cpd.customer_id
left join risk_assessment ra on ab.customer_id = ra.customer_id
left join product_analytics pa on ab.customer_id = pa.customer_id 
{{ config(
    materialized='view',
    tags=['silver', 'intermediate', 'customer_analytics', 'master_view']
) }}

/*
    Customer 360 Master View
    
    The ultimate customer intelligence model combining multiple intermediate layers:
    - Customer profile (int_customer_profile)
    - Financial holdings summary (int_customer_financial_summary)
    - Transaction analytics (int_transaction_analytics) - mapped via accounts
    - Comprehensive risk profile (int_comprehensive_risk_profile)
    
    This creates the complete customer view for executive dashboards and AI/ML.
    Creates beautiful dependency cascades: staging → intermediate → master intermediate
*/

with customer_profile_base as (
    select
        customer_id,
        customer_number,
        full_name,
        first_name,
        last_name,
        age,
        gender,
        age_cohort_name,
        annual_income,
        credit_score,
        income_bracket_name,
        credit_score_range_name,
        risk_level,
        primary_address,
        city,
        state_code,
        customer_status,
        customer_since_date,
        relationship_years,
        lifecycle_stage_name,
        customer_value_segment,
        current_segment,
        marketing_eligible,
        lending_eligible,
        premium_product_eligible,
        engagement_level,
        at_risk_customer,
        digital_preference,
        total_service_interactions,
        avg_satisfaction_score,
        total_digital_activities
    from {{ ref('int_customer_profile') }}
),

financial_summary as (
    select
        customer_id,
        total_deposit_accounts,
        active_deposit_accounts,
        total_deposit_balances,
        total_cards,
        credit_cards,
        total_credit_limit,
        total_credit_balance,
        avg_credit_utilization,
        total_loans,
        total_loan_balance,
        delinquent_loans,
        total_investments,
        total_investment_value,
        retirement_account_value,
        total_insurance_policies,
        total_insurance_coverage,
        net_worth_with_bank,
        total_relationship_value,
        product_penetration_score,
        financial_risk_level,
        wealth_tier
    from {{ ref('int_customer_financial_summary') }}
),

-- Get transaction analytics by aggregating across all customer accounts
customer_transaction_summary as (
    select
        a.customer_id,
        sum(ta.total_transactions) as total_transactions_across_accounts,
        sum(ta.total_transaction_volume) as total_transaction_volume,
        avg(ta.avg_transaction_amount) as avg_transaction_amount,
        max(ta.largest_transaction) as largest_single_transaction,
        sum(ta.net_cash_flow) as total_net_cash_flow,
        sum(ta.total_deposits) as total_deposits,
        sum(ta.total_deposit_amount) as total_deposit_amount,
        sum(ta.total_withdrawals) as total_withdrawals,
        sum(ta.total_withdrawal_amount) as total_withdrawal_amount,
        sum(ta.total_payments) as total_payments,
        sum(ta.total_payment_amount) as total_payment_amount,
        sum(ta.total_fees) as total_fees_paid,
        sum(ta.total_fee_amount) as total_fee_amount_paid,
        avg(ta.digital_transaction_percentage) as avg_digital_transaction_pct,
        avg(ta.fee_to_volume_ratio) as avg_fee_to_volume_ratio,
        max(ta.last_transaction_date) as last_transaction_date,
        mode() within group (order by ta.spending_category) as primary_spending_category,
        mode() within group (order by ta.activity_level) as primary_activity_level
    from {{ ref('stg_accounts') }} a
    left join {{ ref('int_transaction_analytics') }} ta on a.account_id = ta.account_id
    group by a.customer_id
),

risk_profile as (
    select
        customer_id,
        current_credit_risk_score,
        current_fraud_risk_score,
        overall_risk_level,
        compliance_status,
        composite_risk_score,
        total_fraud_alerts,
        confirmed_fraud_incidents,
        total_compliance_flags,
        avg_credit_score as risk_avg_credit_score,
        requires_enhanced_monitoring,
        requires_immediate_review,
        risk_trend
    from {{ ref('int_comprehensive_risk_profile') }}
)

select
    -- Customer Identity & Demographics
    cp.customer_id,
    cp.customer_number,
    cp.full_name,
    cp.first_name,
    cp.last_name,
    cp.age,
    cp.gender,
    cp.age_cohort_name,
    cp.primary_address,
    cp.city,
    cp.state_code,
    
    -- Financial Profile
    cp.annual_income,
    cp.credit_score,
    cp.income_bracket_name,
    cp.credit_score_range_name,
    cp.risk_level,
    
    -- Customer Lifecycle
    cp.customer_status,
    cp.customer_since_date,
    cp.relationship_years,
    cp.lifecycle_stage_name,
    cp.customer_value_segment,
    cp.current_segment,
    
    -- Product Holdings Summary
    coalesce(fs.total_deposit_accounts, 0) as total_deposit_accounts,
    coalesce(fs.total_deposit_balances, 0) as total_deposit_balances,
    coalesce(fs.total_cards, 0) as total_cards,
    coalesce(fs.total_credit_limit, 0) as total_credit_limit,
    coalesce(fs.total_loans, 0) as total_loans,
    coalesce(fs.total_loan_balance, 0) as total_loan_balance,
    coalesce(fs.total_investments, 0) as total_investments,
    coalesce(fs.total_investment_value, 0) as total_investment_value,
    coalesce(fs.total_insurance_policies, 0) as total_insurance_policies,
    
    -- Wealth & Relationship Value
    coalesce(fs.net_worth_with_bank, 0) as net_worth_with_bank,
    coalesce(fs.total_relationship_value, 0) as total_relationship_value,
    coalesce(fs.product_penetration_score, 0) as product_penetration_score,
    fs.wealth_tier,
    
    -- Transaction Behavior
    coalesce(cts.total_transactions_across_accounts, 0) as total_transactions,
    coalesce(cts.total_transaction_volume, 0) as total_transaction_volume,
    coalesce(cts.total_net_cash_flow, 0) as net_cash_flow,
    coalesce(cts.total_deposit_amount, 0) as total_deposits_amount,
    coalesce(cts.total_payment_amount, 0) as total_payments_amount,
    coalesce(cts.total_fee_amount_paid, 0) as total_fees_paid,
    cts.last_transaction_date,
    cts.primary_spending_category,
    cts.primary_activity_level,
    
    -- Digital Engagement
    cp.digital_preference,
    cp.total_digital_activities,
    coalesce(cts.avg_digital_transaction_pct, 0) as digital_transaction_percentage,
    
    -- Service Experience
    cp.engagement_level,
    cp.total_service_interactions,
    cp.avg_satisfaction_score,
    
    -- Risk Assessment
    coalesce(rp.overall_risk_level, 'UNKNOWN') as overall_risk_level,
    coalesce(rp.compliance_status, 'PENDING_VERIFICATION') as compliance_status,
    coalesce(rp.composite_risk_score, 500) as composite_risk_score,
    coalesce(rp.current_credit_risk_score, 500) as current_credit_risk_score,
    coalesce(rp.current_fraud_risk_score, 500) as current_fraud_risk_score,
    coalesce(rp.total_fraud_alerts, 0) as total_fraud_alerts,
    coalesce(rp.confirmed_fraud_incidents, 0) as confirmed_fraud_incidents,
    coalesce(rp.requires_enhanced_monitoring, false) as requires_enhanced_monitoring,
    coalesce(rp.requires_immediate_review, false) as requires_immediate_review,
    
    -- Eligibility Flags
    cp.marketing_eligible,
    cp.lending_eligible,
    cp.premium_product_eligible,
    
    -- Advanced Scoring & Classifications
    
    -- Customer Lifetime Value Score (0-100)
    least(100, greatest(0, 
        (coalesce(fs.product_penetration_score, 0) * 0.3) +
        (case 
            when fs.total_relationship_value >= 1000000 then 40
            when fs.total_relationship_value >= 250000 then 30
            when fs.total_relationship_value >= 100000 then 20
            when fs.total_relationship_value >= 25000 then 10
            else 5
        end) +
        (case 
            when cp.relationship_years >= 10 then 20
            when cp.relationship_years >= 5 then 15
            when cp.relationship_years >= 2 then 10
            else 5
        end) +
        (case 
            when cp.engagement_level = 'HIGHLY_ENGAGED' then 10
            when cp.engagement_level = 'MODERATELY_ENGAGED' then 7
            when cp.engagement_level = 'LIGHTLY_ENGAGED' then 4
            else 1
        end)
    )) as customer_lifetime_value_score,
    
    -- Churn Risk Score (0-100, higher = more likely to churn)
    least(100, greatest(0,
        (case when cp.at_risk_customer then 40 else 0 end) +
        (case when cp.avg_satisfaction_score < 3 then 20 else 0 end) +
        (case when fs.delinquent_loans > 0 then 15 else 0 end) +
        (case when cts.total_transactions_across_accounts = 0 then 15 else 0 end) +
        (case when rp.overall_risk_level in ('HIGH_RISK', 'CRITICAL_RISK') then 10 else 0 end)
    )) as churn_risk_score,
    
    -- Next Best Action Recommendation
    case 
        when rp.requires_immediate_review then 'IMMEDIATE_RISK_REVIEW'
        when cp.at_risk_customer and cp.avg_satisfaction_score < 3 then 'RETENTION_OUTREACH'
        when fs.wealth_tier in ('PRIVATE_BANKING', 'WEALTH_MANAGEMENT') and fs.total_investments = 0 then 'WEALTH_CONSULTATION'
        when fs.product_penetration_score < 40 and cp.lending_eligible then 'CROSS_SELL_LENDING'
        when cts.avg_digital_transaction_pct < 50 and cp.age < 50 then 'DIGITAL_ADOPTION_CAMPAIGN'
        when fs.avg_credit_utilization > 0.8 then 'CREDIT_LIMIT_INCREASE_OFFER'
        when cp.engagement_level = 'MINIMAL_ENGAGEMENT' then 'ENGAGEMENT_CAMPAIGN'
        else 'MAINTAIN_RELATIONSHIP'
    end as next_best_action,
    
    -- Executive Summary Tier
    case 
        when fs.wealth_tier = 'PRIVATE_BANKING' then 'ULTRA_HIGH_NET_WORTH'
        when fs.wealth_tier = 'WEALTH_MANAGEMENT' then 'HIGH_NET_WORTH'
        when fs.product_penetration_score >= 80 and cp.relationship_years >= 5 then 'PLATINUM_CUSTOMER'
        when fs.product_penetration_score >= 60 and cp.engagement_level = 'HIGHLY_ENGAGED' then 'GOLD_CUSTOMER'
        when fs.product_penetration_score >= 40 then 'SILVER_CUSTOMER'
        else 'BRONZE_CUSTOMER'
    end as executive_customer_tier,
    
    current_timestamp as last_updated

from customer_profile_base cp
left join financial_summary fs on cp.customer_id = fs.customer_id
left join customer_transaction_summary cts on cp.customer_id = cts.customer_id
left join risk_profile rp on cp.customer_id = rp.customer_id 
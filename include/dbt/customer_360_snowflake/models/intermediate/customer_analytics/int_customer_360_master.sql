{{ config(
    materialized='view',
    tags=['silver', 'intermediate', 'customer_analytics', 'master_view']
) }}

/*
    Customer 360 Master View

    Converts original model to Snowflake-compatible SQL.
    Logic, column names, and refs are unchanged.
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
        customer_risk_profile as financial_risk_level,  -- Updated column name
        customer_value_segment as wealth_tier  -- Updated column name
    from {{ ref('int_customer_financial_summary') }}
),

customer_transaction_summary as (
    select
        a.customer_id,
        sum(ta.total_transactions)               as total_transactions_across_accounts,
        sum(ta.total_transaction_volume)         as total_transaction_volume,
        avg(ta.avg_transaction_amount)           as avg_transaction_amount,
        max(ta.largest_transaction)              as largest_single_transaction,
        sum(ta.net_cash_flow)                    as total_net_cash_flow,
        sum(ta.total_deposits)                   as total_deposits,
        sum(ta.total_deposit_amount)             as total_deposit_amount,
        sum(ta.total_withdrawals)                as total_withdrawals,
        sum(ta.total_withdrawal_amount)          as total_withdrawal_amount,
        sum(ta.total_payments)                   as total_payments,
        sum(ta.total_payment_amount)             as total_payment_amount,
        sum(ta.total_fees)                       as total_fees_paid,
        sum(ta.total_fee_amount)                 as total_fee_amount_paid,
        avg(ta.digital_transaction_percentage)   as avg_digital_transaction_pct,
        avg(ta.fee_to_volume_ratio)              as avg_fee_to_volume_ratio,
        max(ta.last_transaction_date)            as last_transaction_date,
        MODE(ta.spending_category)               as primary_spending_category,
        MODE(ta.activity_level)                  as primary_activity_level
    from {{ ref('stg_accounts') }} a
    left join {{ ref('int_transaction_analytics') }} ta
        on a.account_id = ta.account_id
    group by a.customer_id
),

risk_profile as (
    select
        customer_id,
        current_credit_risk_score,
        current_fraud_risk_score,
        composite_risk_rating as overall_risk_level,  -- Updated column name
        compliance_status,
        composite_risk_score,
        total_fraud_alerts,
        confirmed_fraud_incidents,
        total_compliance_flags,
        avg_credit_score as risk_avg_credit_score,
        case when overall_risk_assessment = 'REQUIRES_MONITORING' then true else false end as requires_enhanced_monitoring,  -- Derived
        case when overall_risk_assessment = 'REQUIRES_IMMEDIATE_REVIEW' then true else false end as requires_immediate_review,  -- Derived
        'STABLE' as risk_trend  -- Default value since this column doesn't exist in the updated model
    from {{ ref('int_comprehensive_risk_profile') }}
)

select
    /* Customer Identity & Demographics */
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

    /* Financial Profile */
    cp.annual_income,
    cp.credit_score,
    cp.income_bracket_name,
    cp.credit_score_range_name,
    cp.risk_level,

    /* Customer Lifecycle */
    cp.customer_status,
    cp.customer_since_date,
    cp.relationship_years,
    cp.lifecycle_stage_name,
    cp.customer_value_segment,
    cp.current_segment,

    /* Product Holdings Summary */
    COALESCE(fs.total_deposit_accounts,   0) as total_deposit_accounts,
    COALESCE(fs.total_deposit_balances,   0) as total_deposit_balances,
    COALESCE(fs.total_cards,              0) as total_cards,
    COALESCE(fs.total_credit_limit,       0) as total_credit_limit,
    COALESCE(fs.total_loans,              0) as total_loans,
    COALESCE(fs.total_loan_balance,       0) as total_loan_balance,
    COALESCE(fs.total_investments,        0) as total_investments,
    COALESCE(fs.total_investment_value,   0) as total_investment_value,
    COALESCE(fs.total_insurance_policies, 0) as total_insurance_policies,

    /* Wealth & Relationship Value */
    COALESCE(fs.net_worth_with_bank,      0) as net_worth_with_bank,
    COALESCE(fs.total_relationship_value, 0) as total_relationship_value,
    COALESCE(fs.product_penetration_score,0) as product_penetration_score,
    fs.wealth_tier,

    /* Transaction Behavior */
    COALESCE(cts.total_transactions_across_accounts, 0) as total_transactions,
    COALESCE(cts.total_transaction_volume,           0) as total_transaction_volume,
    COALESCE(cts.total_net_cash_flow,                0) as net_cash_flow,
    COALESCE(cts.total_deposit_amount,               0) as total_deposits_amount,
    COALESCE(cts.total_payment_amount,               0) as total_payments_amount,
    COALESCE(cts.total_fee_amount_paid,              0) as total_fees_paid,
    cts.last_transaction_date,
    cts.primary_spending_category,
    cts.primary_activity_level,

    /* Digital Engagement */
    cp.digital_preference,
    cp.total_digital_activities,
    COALESCE(cts.avg_digital_transaction_pct, 0) as digital_transaction_percentage,

    /* Service Experience */
    cp.engagement_level,
    cp.total_service_interactions,
    cp.avg_satisfaction_score,

    /* Risk Assessment */
    COALESCE(rp.overall_risk_level,        'UNKNOWN')            as overall_risk_level,
    COALESCE(rp.compliance_status,         'COMPLIANT') as compliance_status,
    COALESCE(rp.composite_risk_score,      500)                  as composite_risk_score,
    COALESCE(rp.current_credit_risk_score, 500)                  as current_credit_risk_score,
    COALESCE(rp.current_fraud_risk_score,  500)                  as current_fraud_risk_score,
    COALESCE(rp.total_fraud_alerts,        0)                    as total_fraud_alerts,
    COALESCE(rp.confirmed_fraud_incidents, 0)                    as confirmed_fraud_incidents,
    COALESCE(rp.requires_enhanced_monitoring, FALSE)             as requires_enhanced_monitoring,
    COALESCE(rp.requires_immediate_review,  FALSE)               as requires_immediate_review,

    /* Eligibility Flags */
    cp.marketing_eligible,
    cp.lending_eligible,
    cp.premium_product_eligible,

    /* Advanced Scoring & Classifications */
    LEAST(100, GREATEST(0,
        (COALESCE(fs.product_penetration_score, 0) * 0.3) +
        CASE
            WHEN fs.total_relationship_value >= 1000000 THEN 40
            WHEN fs.total_relationship_value >= 250000  THEN 30
            WHEN fs.total_relationship_value >= 100000  THEN 20
            WHEN fs.total_relationship_value >= 25000   THEN 10
            ELSE 5
        END +
        CASE
            WHEN cp.relationship_years >= 10 THEN 20
            WHEN cp.relationship_years >= 5  THEN 15
            WHEN cp.relationship_years >= 2  THEN 10
            ELSE 5
        END +
        CASE
            WHEN cp.engagement_level = 'HIGHLY_ENGAGED'      THEN 10
            WHEN cp.engagement_level = 'MODERATELY_ENGAGED'  THEN 7
            WHEN cp.engagement_level = 'LIGHTLY_ENGAGED'     THEN 4
            ELSE 1
        END
    )) as customer_lifetime_value_score,

    LEAST(100, GREATEST(0,
        CASE WHEN cp.at_risk_customer THEN 40 ELSE 0 END +
        CASE WHEN cp.avg_satisfaction_score < 3 THEN 20 ELSE 0 END +
        CASE WHEN fs.delinquent_loans > 0        THEN 15 ELSE 0 END +
        CASE WHEN cts.total_transactions_across_accounts = 0 THEN 15 ELSE 0 END +
        CASE WHEN rp.overall_risk_level = 'HIGH_RISK' THEN 10 ELSE 0 END
    )) as churn_risk_score,

    CASE
        WHEN rp.requires_immediate_review                                       THEN 'IMMEDIATE_RISK_REVIEW'
        WHEN cp.at_risk_customer AND cp.avg_satisfaction_score < 3              THEN 'RETENTION_OUTREACH'
        WHEN fs.wealth_tier IN ('HIGH_VALUE','MEDIUM_VALUE')
             AND fs.total_investments = 0                                       THEN 'WEALTH_CONSULTATION'
        WHEN fs.product_penetration_score < 40 AND cp.lending_eligible          THEN 'CROSS_SELL_LENDING'
        WHEN cts.avg_digital_transaction_pct < 50 AND cp.age < 50              THEN 'DIGITAL_ADOPTION_CAMPAIGN'
        WHEN fs.avg_credit_utilization > 0.8                                    THEN 'CREDIT_LIMIT_INCREASE_OFFER'
        WHEN cp.engagement_level = 'MINIMAL_ENGAGEMENT'                         THEN 'ENGAGEMENT_CAMPAIGN'
        ELSE 'MAINTAIN_RELATIONSHIP'
    END as next_best_action,

    CASE
        WHEN fs.wealth_tier = 'HIGH_VALUE'                                      THEN 'ULTRA_HIGH_NET_WORTH'
        WHEN fs.wealth_tier = 'MEDIUM_VALUE'                                    THEN 'HIGH_NET_WORTH'
        WHEN fs.product_penetration_score >= 80
             AND cp.relationship_years >= 5                                     THEN 'PLATINUM_CUSTOMER'
        WHEN fs.product_penetration_score >= 60
             AND cp.engagement_level = 'HIGHLY_ENGAGED'                         THEN 'GOLD_CUSTOMER'
        WHEN fs.product_penetration_score >= 40                                 THEN 'SILVER_CUSTOMER'
        ELSE 'BRONZE_CUSTOMER'
    END as executive_customer_tier,

    CURRENT_TIMESTAMP() as last_updated

from customer_profile_base cp
left join financial_summary           fs  on cp.customer_id = fs.customer_id
left join customer_transaction_summary cts on cp.customer_id = cts.customer_id
left join risk_profile                rp  on cp.customer_id = rp.customer_id
{{ config(
    materialized='view',
    tags=['silver', 'intermediate', 'risk_analytics', 'compliance']
) }}

/*
    Comprehensive Risk Profile
    
    Integrated risk assessment combining:
    - Risk assessments (stg_risk_assessments)
    - Fraud detection alerts (stg_fraud_alerts)
    - Compliance screening (stg_compliance_records)
    - Credit score history (stg_credit_scores)
    - KYC compliance (stg_kyc_records)
    - Account alerts (stg_account_alerts)
    
    Provides unified risk scoring and regulatory compliance status.
*/

with risk_assessment_ranked as (
    select
        customer_id,
        risk_type,
        risk_score,
        risk_level as risk_rating,
        assessment_date,
        case when risk_level in ('HIGH', 'MEDIUM') then true else false end as requires_review,
        row_number() over (partition by customer_id, risk_type order by assessment_date desc) as rn
    from {{ ref('stg_risk_assessments') }}
),

latest_risk_assessment as (
    select
        customer_id,
        risk_type,
        risk_score,
        risk_rating,
        assessment_date,
        requires_review
    from risk_assessment_ranked
    where rn = 1
),

risk_summary as (
    select
        customer_id,
        count(*) as total_risk_assessments,
        max(case when risk_type = 'CREDIT_RISK' then risk_score end) as latest_credit_risk_score,
        max(case when risk_type = 'FRAUD_RISK' then risk_score end) as latest_fraud_risk_score,
        max(case when risk_type = 'OPERATIONAL_RISK' then risk_score end) as latest_operational_risk_score,
        max(case when risk_type = 'COMPLIANCE_RISK' then risk_score end) as latest_market_risk_score,
        max(case when risk_type = 'CREDIT_RISK' then risk_rating end) as credit_risk_rating,
        max(case when risk_type = 'FRAUD_RISK' then risk_rating end) as fraud_risk_rating,
        max(assessment_date) as last_risk_assessment_date,
        sum(case when requires_review then 1 else 0 end) as assessments_requiring_review
    from latest_risk_assessment
    group by customer_id
),

fraud_summary as (
    select
        customer_id,
        count(*) as total_fraud_alerts,
        count(case when alert_status = 'CONFIRMED_FRAUD' then 1 end) as confirmed_fraud_alerts,
        count(case when alert_status = 'FALSE_POSITIVE' then 1 end) as false_positive_alerts,
        count(case when alert_status = 'UNDER_REVIEW' then 1 end) as pending_fraud_alerts,
        max(case when risk_level = 'HIGH' then 850 when risk_level = 'MEDIUM' then 650 else 450 end) as highest_fraud_risk_score,
        avg(case when risk_level = 'HIGH' then 850 when risk_level = 'MEDIUM' then 650 else 450 end) as avg_fraud_risk_score,
        max(alert_timestamp::date) as last_fraud_alert_date,
        count(case when alert_type = 'UNUSUAL_SPENDING' then 1 end) as unusual_transaction_alerts,
        count(case when alert_type = 'VELOCITY_CHECK' then 1 end) as velocity_alerts
    from {{ ref('stg_fraud_alerts') }}
    group by customer_id
),

compliance_summary as (
    select
        customer_id,
        count(*) as total_compliance_checks,
        count(case when compliance_status = 'FLAGGED' then 1 end) as compliance_flags,
        max(check_date) as last_compliance_check_date,
        count(case when compliance_type = 'AML_SCREENING' then 1 end) as aml_checks,
        count(case when compliance_type = 'SANCTIONS_CHECK' then 1 end) as sanctions_checks,
        count(case when compliance_type = 'PEP_SCREENING' then 1 end) as pep_checks,
        sum(case when compliance_type = 'AML_SCREENING' and compliance_status = 'FLAGGED' then 1 else 0 end) as aml_flags,
        sum(case when compliance_type = 'SANCTIONS_CHECK' and compliance_status = 'FLAGGED' then 1 else 0 end) as sanctions_flags
    from {{ ref('stg_compliance_records') }}
    group by customer_id
),

credit_score_summary as (
    select
        customer_id,
        count(*) as total_credit_reports,
        max(score_value) as highest_credit_score,
        min(score_value) as lowest_credit_score,
        avg(score_value) as avg_credit_score,
        max(score_date) as last_credit_score_date,
        max(case when bureau = 'EXPERIAN' then score_value end) as latest_experian_score,
        max(case when bureau = 'EQUIFAX' then score_value end) as latest_equifax_score,
        max(case when bureau = 'TRANSUNION' then score_value end) as latest_transunion_score,
        stddev(score_value) as credit_score_volatility
    from {{ ref('stg_credit_scores') }}
    group by customer_id
),

kyc_summary as (
    select
        customer_id,
        count(*) as total_kyc_checks,
        count(case when verification_status = 'VERIFIED' then 1 end) as passed_kyc_checks,
        count(case when verification_status = 'FAILED' then 1 end) as failed_kyc_checks,
        max(verification_date) as last_kyc_check_date,
        count(case when kyc_type = 'ENHANCED' then 1 end) as enhanced_dd_checks,
        max(case when kyc_type = 'INITIAL' then verification_date end) as initial_kyc_date
    from {{ ref('stg_kyc_records') }}
    group by customer_id
),

account_alert_summary as (
    select
        a.customer_id,
        count(aa.alert_id) as total_account_alerts,
        count(case when aa.alert_type = 'UNUSUAL_ACTIVITY' then 1 end) as unusual_activity_alerts,
        count(case when aa.alert_type = 'LOW_BALANCE' then 1 end) as low_balance_alerts,
        count(case when aa.alert_status = 'PENDING' then 1 end) as pending_alerts,
        max(aa.alert_timestamp::date) as last_account_alert_date
    from {{ ref('stg_accounts') }} a
    left join {{ ref('stg_account_alerts') }} aa on a.account_id = aa.account_id
    group by a.customer_id
)

select
    -- Customer identifier
    coalesce(
        rs.customer_id,
        fs.customer_id,
        cs.customer_id,
        css.customer_id,
        ks.customer_id,
        aas.customer_id
    ) as customer_id,
    
    -- Risk Assessment Scores
    coalesce(rs.latest_credit_risk_score, 500) as current_credit_risk_score,
    coalesce(rs.latest_fraud_risk_score, 500) as current_fraud_risk_score,
    coalesce(rs.latest_operational_risk_score, 500) as current_operational_risk_score,
    coalesce(rs.latest_market_risk_score, 500) as current_market_risk_score,
    rs.credit_risk_rating,
    rs.fraud_risk_rating,
    rs.last_risk_assessment_date,
    
    -- Fraud Metrics
    coalesce(fs.total_fraud_alerts, 0) as total_fraud_alerts,
    coalesce(fs.confirmed_fraud_alerts, 0) as confirmed_fraud_incidents,
    coalesce(fs.false_positive_alerts, 0) as false_positive_alerts,
    coalesce(fs.pending_fraud_alerts, 0) as pending_fraud_investigations,
    coalesce(fs.highest_fraud_risk_score, 0) as highest_fraud_risk_score,
    fs.last_fraud_alert_date,
    
    -- Compliance Status
    coalesce(cs.total_compliance_checks, 0) as total_compliance_checks,
    coalesce(cs.compliance_flags, 0) as total_compliance_flags,
    coalesce(cs.aml_flags, 0) as aml_flags,
    coalesce(cs.sanctions_flags, 0) as sanctions_flags,
    cs.last_compliance_check_date,
    
    -- Credit Profile
    coalesce(css.avg_credit_score, 500) as avg_credit_score,
    css.highest_credit_score,
    css.lowest_credit_score,
    css.latest_experian_score,
    css.latest_equifax_score,
    css.latest_transunion_score,
    coalesce(css.credit_score_volatility, 0) as credit_score_volatility,
    css.last_credit_score_date,
    
    -- KYC Compliance
    coalesce(ks.total_kyc_checks, 0) as total_kyc_checks,
    coalesce(ks.passed_kyc_checks, 0) as passed_kyc_checks,
    coalesce(ks.failed_kyc_checks, 0) as failed_kyc_checks,
    ks.last_kyc_check_date,
    ks.initial_kyc_date,
    
    -- Account Monitoring
    coalesce(aas.total_account_alerts, 0) as total_account_alerts,
    coalesce(aas.unusual_activity_alerts, 0) as unusual_activity_alerts,
    coalesce(aas.pending_alerts, 0) as pending_account_alerts,
    aas.last_account_alert_date,
    
    -- Composite Risk Scores
    (
        coalesce(rs.latest_credit_risk_score, 500) * 0.3 +
        coalesce(rs.latest_fraud_risk_score, 500) * 0.3 +
        coalesce(css.avg_credit_score, 500) * 0.2 +
        (850 - coalesce(fs.highest_fraud_risk_score, 0)) * 0.2
    ) as composite_risk_score,
    
    -- Risk Classifications
    case 
        when (
            coalesce(rs.latest_credit_risk_score, 500) * 0.3 +
            coalesce(rs.latest_fraud_risk_score, 500) * 0.3 +
            coalesce(css.avg_credit_score, 500) * 0.2 +
            (850 - coalesce(fs.highest_fraud_risk_score, 0)) * 0.2
        ) >= 750 then 'LOW_RISK'
        when (
            coalesce(rs.latest_credit_risk_score, 500) * 0.3 +
            coalesce(rs.latest_fraud_risk_score, 500) * 0.3 +
            coalesce(css.avg_credit_score, 500) * 0.2 +
            (850 - coalesce(fs.highest_fraud_risk_score, 0)) * 0.2
        ) >= 600 then 'MEDIUM_RISK'
        else 'HIGH_RISK'
    end as composite_risk_rating,
    
    -- Compliance Flags
    case 
        when coalesce(cs.compliance_flags, 0) = 0 then 'COMPLIANT'
        when coalesce(cs.compliance_flags, 0) <= 2 then 'MINOR_CONCERNS'
        else 'MAJOR_CONCERNS'
    end as compliance_status,
    
    -- Fraud Risk Level
    case 
        when coalesce(fs.confirmed_fraud_alerts, 0) > 0 then 'HIGH_FRAUD_RISK'
        when coalesce(fs.pending_fraud_alerts, 0) > 2 then 'MEDIUM_FRAUD_RISK'
        else 'LOW_FRAUD_RISK'
    end as fraud_risk_level,
    
    -- KYC Status
    case 
        when coalesce(ks.failed_kyc_checks, 0) > 0 then 'KYC_FAILED'
        when coalesce(ks.passed_kyc_checks, 0) > 0 then 'KYC_PASSED'
        else 'KYC_PENDING'
    end as kyc_status,
    
    -- Overall Risk Assessment
    case 
        when coalesce(cs.compliance_flags, 0) > 2 
             or coalesce(fs.confirmed_fraud_alerts, 0) > 0 
             or coalesce(ks.failed_kyc_checks, 0) > 0 then 'REQUIRES_IMMEDIATE_REVIEW'
        when coalesce(cs.compliance_flags, 0) > 0 
             or coalesce(fs.pending_fraud_alerts, 0) > 1 then 'REQUIRES_MONITORING'
        else 'STANDARD_MONITORING'
    end as overall_risk_assessment,
    
    current_timestamp as last_updated
    
from risk_summary rs
full outer join fraud_summary fs on rs.customer_id = fs.customer_id
full outer join compliance_summary cs on coalesce(rs.customer_id, fs.customer_id) = cs.customer_id
full outer join credit_score_summary css on coalesce(rs.customer_id, fs.customer_id, cs.customer_id) = css.customer_id
full outer join kyc_summary ks on coalesce(rs.customer_id, fs.customer_id, cs.customer_id, css.customer_id) = ks.customer_id
full outer join account_alert_summary aas on coalesce(rs.customer_id, fs.customer_id, cs.customer_id, css.customer_id, ks.customer_id) = aas.customer_id 
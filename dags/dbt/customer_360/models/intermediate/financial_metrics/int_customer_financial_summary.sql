{{ config(
    materialized='view',
    tags=['silver', 'intermediate', 'financial_metrics', 'holdings']
) }}

/*
    Customer Financial Holdings Summary
    
    Comprehensive view of all customer financial relationships:
    - Deposit accounts (stg_accounts)
    - Credit & debit cards (stg_cards) 
    - Loan products (stg_loans)
    - Investment holdings (stg_investments)
    - Insurance policies (stg_insurance_policies)
    - Account balances (stg_account_balances)
    
    Provides total relationship value and product penetration metrics.
*/

with account_summary as (
    select
        a.customer_id,
        count(*) as total_accounts,
        count(case when a.account_status = 'ACTIVE' then 1 end) as active_accounts,
        count(case when a.product_type = 'Checking' then 1 end) as checking_accounts,
        count(case when a.product_type = 'Savings' then 1 end) as savings_accounts,
        count(case when a.product_type = 'CD' then 1 end) as cd_accounts,
        count(case when a.product_type = 'Investment' then 1 end) as investment_accounts,
        sum(a.current_balance) as total_deposit_balances,
        avg(a.current_balance) as avg_account_balance,
        max(a.current_balance) as largest_account_balance,
        sum(a.monthly_fee_revenue) as monthly_fee_revenue,
        min(a.opened_date) as first_account_opened_date,
        max(a.opened_date) as latest_account_opened_date
    from {{ ref('stg_accounts') }} a
    group by a.customer_id
),

card_summary as (
    select
        customer_id,
        count(*) as total_cards,
        count(case when card_status = 'ACTIVE' then 1 end) as active_cards,
        count(case when card_type = 'CREDIT' then 1 end) as credit_cards,
        count(case when card_type = 'DEBIT' then 1 end) as debit_cards,
        count(case when card_type = 'PREPAID' then 1 end) as prepaid_cards,
        sum(case when card_type = 'CREDIT' then credit_limit else 0 end) as total_credit_limit,
        sum(case when card_type = 'CREDIT' then current_balance else 0 end) as total_credit_balance,
        avg(case when card_type = 'CREDIT' and current_balance > 0 then utilization_rate end) as avg_credit_utilization,
        max(case when card_type = 'CREDIT' then utilization_rate else 0 end) as max_credit_utilization
    from {{ ref('stg_cards') }}
    group by customer_id
),

loan_summary as (
    select
        customer_id,
        count(*) as total_loans,
        count(case when loan_status = 'CURRENT' then 1 end) as current_loans,
        count(case when loan_type = 'MORTGAGE' then 1 end) as mortgages,
        count(case when loan_type = 'PERSONAL' then 1 end) as personal_loans,
        count(case when loan_type = 'AUTO' then 1 end) as auto_loans,
        count(case when loan_type = 'HOME_EQUITY' then 1 end) as home_equity_loans,
        sum(original_amount) as total_original_loan_amount,
        sum(current_balance) as total_current_loan_balance,
        avg(interest_rate) as avg_loan_interest_rate,
        sum(case when loan_status in ('DELINQUENT', 'DEFAULT') then current_balance else 0 end) as at_risk_loan_balance,
        count(case when loan_status in ('DELINQUENT', 'DEFAULT') then 1 end) as delinquent_loans
    from {{ ref('stg_loans') }}
    group by customer_id
),

investment_summary as (
    select
        customer_id,
        count(*) as total_investments,
        count(distinct investment_type) as investment_types_held,
        sum(current_value) as total_investment_value,
        sum(unrealized_gain_loss) as total_unrealized_gain_loss,
        avg(return_percent) as avg_investment_return,
        count(case when investment_type = 'STOCK' then 1 end) as stock_holdings,
        count(case when investment_type = 'BOND' then 1 end) as bond_holdings,
        count(case when investment_type = 'MUTUAL_FUND' then 1 end) as mutual_fund_holdings,
        count(case when account_type = 'IRA' then 1 end) as retirement_accounts,
        sum(case when account_type in ('IRA', 'ROTH_IRA', '401K') then current_value else 0 end) as retirement_account_value
    from {{ ref('stg_investments') }}
    group by customer_id
),

insurance_summary as (
    select
        customer_id,
        count(*) as total_policies,
        count(case when policy_status = 'ACTIVE' then 1 end) as active_policies,
        count(case when policy_type = 'LIFE' then 1 end) as life_policies,
        count(case when policy_type = 'AUTO' then 1 end) as auto_policies,
        count(case when policy_type = 'HOME' then 1 end) as home_policies,
        count(case when policy_type = 'HEALTH' then 1 end) as health_policies,
        sum(coverage_amount) as total_coverage_amount,
        sum(annual_premium) as total_annual_premiums,
        avg(annual_premium) as avg_policy_premium
    from {{ ref('stg_insurance_policies') }}
    group by customer_id
),

latest_balances_ranked as (
    select
        account_id,
        balance_amount as latest_balance,
        available_balance as latest_available_balance,
        balance_date as latest_balance_date,
        row_number() over (partition by account_id order by balance_date desc) as rn
    from {{ ref('stg_account_balances') }}
),

latest_balances as (
    select
        account_id,
        latest_balance,
        latest_available_balance,
        latest_balance_date
    from latest_balances_ranked
    where rn = 1
)

select
    -- Customer identifier
    coalesce(
        acs.customer_id, 
        cs.customer_id, 
        ls.customer_id, 
        invs.customer_id, 
        ins.customer_id
    ) as customer_id,
    
    -- Deposit Account Metrics
    coalesce(acs.total_accounts, 0) as total_deposit_accounts,
    coalesce(acs.active_accounts, 0) as active_deposit_accounts,
    coalesce(acs.checking_accounts, 0) as checking_accounts,
    coalesce(acs.savings_accounts, 0) as savings_accounts,
    coalesce(acs.cd_accounts, 0) as cd_accounts,
    coalesce(acs.total_deposit_balances, 0) as total_deposit_balances,
    coalesce(acs.monthly_fee_revenue, 0) as monthly_fee_revenue,
    acs.first_account_opened_date,
    
    -- Card Metrics
    coalesce(cs.total_cards, 0) as total_cards,
    coalesce(cs.active_cards, 0) as active_cards,
    coalesce(cs.credit_cards, 0) as credit_cards,
    coalesce(cs.total_credit_limit, 0) as total_credit_limit,
    coalesce(cs.total_credit_balance, 0) as total_credit_balance,
    coalesce(cs.avg_credit_utilization, 0) as avg_credit_utilization,
    
    -- Loan Metrics
    coalesce(ls.total_loans, 0) as total_loans,
    coalesce(ls.current_loans, 0) as current_loans,
    coalesce(ls.total_current_loan_balance, 0) as total_loan_balance,
    coalesce(ls.avg_loan_interest_rate, 0) as avg_loan_interest_rate,
    coalesce(ls.delinquent_loans, 0) as delinquent_loans,
    coalesce(ls.at_risk_loan_balance, 0) as at_risk_loan_balance,
    
    -- Investment Metrics
    coalesce(invs.total_investments, 0) as total_investments,
    coalesce(invs.total_investment_value, 0) as total_investment_value,
    coalesce(invs.total_unrealized_gain_loss, 0) as total_unrealized_gain_loss,
    coalesce(invs.retirement_account_value, 0) as retirement_account_value,
    
    -- Insurance Metrics
    coalesce(ins.total_policies, 0) as total_insurance_policies,
    coalesce(ins.active_policies, 0) as active_insurance_policies,
    coalesce(ins.total_coverage_amount, 0) as total_insurance_coverage,
    coalesce(ins.total_annual_premiums, 0) as total_annual_premiums,
    
    -- Total Relationship Value
    (
        coalesce(acs.total_deposit_balances, 0) + 
        coalesce(invs.total_investment_value, 0) - 
        coalesce(ls.total_current_loan_balance, 0) - 
        coalesce(cs.total_credit_balance, 0)
    ) as net_worth_with_bank,
    
    (
        coalesce(acs.total_deposit_balances, 0) + 
        coalesce(invs.total_investment_value, 0) + 
        coalesce(cs.total_credit_limit, 0)
    ) as total_relationship_value,
    
    -- Product Penetration Score (0-100)
    (
        (case when acs.total_accounts > 0 then 20 else 0 end) +
        (case when cs.total_cards > 0 then 15 else 0 end) +
        (case when ls.total_loans > 0 then 25 else 0 end) +
        (case when invs.total_investments > 0 then 25 else 0 end) +
        (case when ins.total_policies > 0 then 15 else 0 end)
    ) as product_penetration_score,
    
    -- Risk Indicators
    case 
        when ls.delinquent_loans > 0 or cs.max_credit_utilization > 0.9 then 'HIGH_RISK'
        when cs.avg_credit_utilization > 0.7 or ls.at_risk_loan_balance > 0 then 'MEDIUM_RISK'
        else 'LOW_RISK'
    end as financial_risk_level,
    
    -- Customer Value Tier
    case 
        when (
            coalesce(acs.total_deposit_balances, 0) + 
            coalesce(invs.total_investment_value, 0)
        ) >= 1000000 then 'PRIVATE_BANKING'
        when (
            coalesce(acs.total_deposit_balances, 0) + 
            coalesce(invs.total_investment_value, 0)
        ) >= 250000 then 'WEALTH_MANAGEMENT'
        when (
            coalesce(acs.total_deposit_balances, 0) + 
            coalesce(invs.total_investment_value, 0)
        ) >= 100000 then 'PREFERRED'
        when (
            coalesce(acs.total_deposit_balances, 0) + 
            coalesce(invs.total_investment_value, 0)
        ) >= 25000 then 'SELECT'
        else 'STANDARD'
    end as wealth_tier,
    
    current_timestamp as last_updated

from account_summary acs
full outer join card_summary cs on acs.customer_id = cs.customer_id
full outer join loan_summary ls on coalesce(acs.customer_id, cs.customer_id) = ls.customer_id
full outer join investment_summary invs on coalesce(acs.customer_id, cs.customer_id, ls.customer_id) = invs.customer_id
full outer join insurance_summary ins on coalesce(acs.customer_id, cs.customer_id, ls.customer_id, invs.customer_id) = ins.customer_id 
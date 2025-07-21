{{ config(
    materialized='view',
    tags=['silver', 'intermediate', 'financial_metrics', 'transactions']
) }}

/*
    Transaction Analytics Summary
    
    Comprehensive transaction behavior analysis combining:
    - All transactions (stg_transactions)
    - Deposits (stg_deposits)
    - Withdrawals (stg_withdrawals)
    - Transfers (stg_transfers)
    - Payments (stg_payments)
    - Fees (stg_fees)
    
    Provides spending patterns, cash flow analysis, and behavioral insights.
*/

with transaction_summary as (
    select
        t.account_id,
        count(*) as total_transactions,
        sum(t.transaction_amount) as total_transaction_volume,
        avg(t.transaction_amount) as avg_transaction_amount,
        max(t.transaction_amount) as largest_transaction,
        min(t.transaction_amount) as smallest_transaction,
        count(case when t.flow_direction = 'INFLOW' then 1 end) as inflow_transactions,
        count(case when t.flow_direction = 'OUTFLOW' then 1 end) as outflow_transactions,
        sum(case when t.flow_direction = 'INFLOW' then t.transaction_amount else 0 end) as total_inflows,
        sum(case when t.flow_direction = 'OUTFLOW' then t.transaction_amount else 0 end) as total_outflows,
        count(case when t.is_digital_transaction then 1 end) as digital_transactions,
        count(case when t.channel_category = 'DIGITAL' then 1 end) as digital_channel_transactions,
        count(case when t.time_category = 'OFF_HOURS' then 1 end) as off_hours_transactions,
        count(case when t.amount_category = 'LARGE' then 1 end) as large_amount_transactions,
        avg(t.risk_score) as avg_transaction_risk_score,
        max(t.transaction_date) as last_transaction_date,
        min(t.transaction_date) as first_transaction_date
    from {{ ref('stg_transactions') }} t
    where t.transaction_status = 'COMPLETED'
    group by t.account_id
),

deposit_summary as (
    select
        account_id,
        count(*) as total_deposits,
        sum(deposit_amount) as total_deposit_amount,
        avg(deposit_amount) as avg_deposit_amount,
        count(case when channel_type = 'DIGITAL' then 1 end) as digital_deposits,
        count(case when deposit_type = 'PAYROLL' then 1 end) as payroll_deposits,
        sum(case when deposit_type = 'PAYROLL' then deposit_amount else 0 end) as payroll_deposit_amount,
        max(deposit_datetime::date) as last_deposit_date
    from {{ ref('stg_deposits') }}
    where deposit_status = 'CLEARED'
    group by account_id
),

withdrawal_summary as (
    select
        account_id,
        count(*) as total_withdrawals,
        sum(withdrawal_amount) as total_withdrawal_amount,
        avg(withdrawal_amount) as avg_withdrawal_amount,
        count(case when withdrawal_type = 'ATM' then 1 end) as atm_withdrawals,
        count(case when withdrawal_type = 'ONLINE' then 1 end) as online_withdrawals,
        count(case when amount_category = 'LARGE' then 1 end) as large_withdrawals,
        max(withdrawal_datetime::date) as last_withdrawal_date
    from {{ ref('stg_withdrawals') }}
    where withdrawal_status = 'COMPLETED'
    group by account_id
),

outbound_transfers as (
    select
        from_account_id as account_id,
        count(*) as outbound_transfer_count,
        sum(transfer_amount) as outbound_transfer_amount,
        avg(transfer_amount) as avg_outbound_transfer_amount
    from {{ ref('stg_transfers') }}
    where transfer_status = 'COMPLETED'
    group by from_account_id
),

inbound_transfers as (
    select
        to_account_id as account_id,
        count(*) as inbound_transfer_count,
        sum(transfer_amount) as inbound_transfer_amount,
        avg(transfer_amount) as avg_inbound_transfer_amount
    from {{ ref('stg_transfers') }}
    where transfer_status = 'COMPLETED'
    group by to_account_id
),

transfer_combined as (
    select
        coalesce(ot.account_id, it.account_id) as account_id,
        coalesce(ot.outbound_transfer_count, 0) as total_outbound_transfers,
        coalesce(it.inbound_transfer_count, 0) as total_inbound_transfers,
        coalesce(ot.outbound_transfer_amount, 0) as total_outbound_transfer_amount,
        coalesce(it.inbound_transfer_amount, 0) as total_inbound_transfer_amount
    from outbound_transfers ot
    full outer join inbound_transfers it on ot.account_id = it.account_id
),

payment_summary as (
    select
        account_id,
        count(*) as total_payments,
        sum(payment_amount) as total_payment_amount,
        avg(payment_amount) as avg_payment_amount,
        count(case when payment_type = 'BILL_PAY' then 1 end) as bill_payments,
        count(case when payment_type = 'PERSON_TO_PERSON' then 1 end) as p2p_payments,
        max(payment_date) as last_payment_date
    from {{ ref('stg_payments') }}
    where payment_status = 'COMPLETED'
    group by account_id
),

fee_summary as (
    select
        account_id,
        count(*) as total_fees,
        sum(fee_amount) as total_fee_amount,
        avg(fee_amount) as avg_fee_amount,
        count(case when fee_classification = 'PENALTY' then 1 end) as penalty_fees,
        sum(case when fee_classification = 'PENALTY' then fee_amount else 0 end) as penalty_fee_amount,
        count(case when fee_status = 'WAIVED' then 1 end) as waived_fees,
        max(fee_date) as last_fee_date
    from {{ ref('stg_fees') }}
    where fee_status = 'CHARGED'
    group by account_id
)

select
    -- Account identifier
    coalesce(
        ts.account_id,
        ds.account_id,
        ws.account_id,
        tc.account_id,
        ps.account_id,
        fs.account_id
    ) as account_id,
    
    -- Overall Transaction Metrics
    coalesce(ts.total_transactions, 0) as total_transactions,
    coalesce(ts.total_transaction_volume, 0) as total_transaction_volume,
    coalesce(ts.avg_transaction_amount, 0) as avg_transaction_amount,
    ts.largest_transaction,
    ts.first_transaction_date,
    ts.last_transaction_date,
    
    -- Cash Flow Analysis
    coalesce(ts.total_inflows, 0) as total_inflows,
    coalesce(ts.total_outflows, 0) as total_outflows,
    (coalesce(ts.total_inflows, 0) - coalesce(ts.total_outflows, 0)) as net_cash_flow,
    
    -- Specific Transaction Types
    coalesce(ds.total_deposits, 0) as total_deposits,
    coalesce(ds.total_deposit_amount, 0) as total_deposit_amount,
    coalesce(ds.payroll_deposits, 0) as payroll_deposits,
    coalesce(ds.payroll_deposit_amount, 0) as payroll_deposit_amount,
    
    coalesce(ws.total_withdrawals, 0) as total_withdrawals,
    coalesce(ws.total_withdrawal_amount, 0) as total_withdrawal_amount,
    coalesce(ws.atm_withdrawals, 0) as atm_withdrawals,
    
    coalesce(tc.total_outbound_transfers, 0) as outbound_transfers,
    coalesce(tc.total_inbound_transfers, 0) as inbound_transfers,
    coalesce(tc.total_outbound_transfer_amount, 0) as outbound_transfer_amount,
    coalesce(tc.total_inbound_transfer_amount, 0) as inbound_transfer_amount,
    
    coalesce(ps.total_payments, 0) as total_payments,
    coalesce(ps.total_payment_amount, 0) as total_payment_amount,
    coalesce(ps.bill_payments, 0) as bill_payments,
    coalesce(ps.p2p_payments, 0) as p2p_payments,
    
    -- Fee Analysis
    coalesce(fs.total_fees, 0) as total_fees,
    coalesce(fs.total_fee_amount, 0) as total_fee_amount,
    coalesce(fs.penalty_fees, 0) as penalty_fees,
    coalesce(fs.penalty_fee_amount, 0) as penalty_fee_amount,
    
    -- Digital Adoption Metrics
    coalesce(ts.digital_transactions, 0) as digital_transactions,
    case 
        when ts.total_transactions > 0 then 
            round(ts.digital_transactions::numeric / ts.total_transactions * 100, 2)
        else 0 
    end as digital_transaction_percentage,
    
    coalesce(ds.digital_deposits, 0) as digital_deposits,
    case 
        when ds.total_deposits > 0 then 
            round(ds.digital_deposits::numeric / ds.total_deposits * 100, 2)
        else 0 
    end as digital_deposit_percentage,
    
    -- Behavioral Patterns
    coalesce(ts.off_hours_transactions, 0) as off_hours_transactions,
    coalesce(ts.large_amount_transactions, 0) as large_amount_transactions,
    coalesce(ts.avg_transaction_risk_score, 0) as avg_transaction_risk_score,
    
    -- Transaction Frequency Analysis
    case 
        when ts.last_transaction_date is not null and ts.first_transaction_date is not null then
            ts.total_transactions::numeric / greatest(
                (ts.last_transaction_date - ts.first_transaction_date)::int + 1, 1
            )
        else 0
    end as avg_daily_transaction_frequency,
    
    -- Spending Categories
    case 
        when coalesce(ts.total_outflows, 0) >= 10000 then 'HIGH_SPENDER'
        when coalesce(ts.total_outflows, 0) >= 5000 then 'MEDIUM_SPENDER'
        when coalesce(ts.total_outflows, 0) >= 1000 then 'MODERATE_SPENDER'
        else 'LOW_SPENDER'
    end as spending_category,
    
    -- Account Activity Level
    case 
        when coalesce(ts.total_transactions, 0) >= 100 then 'VERY_ACTIVE'
        when coalesce(ts.total_transactions, 0) >= 50 then 'ACTIVE'
        when coalesce(ts.total_transactions, 0) >= 20 then 'MODERATE'
        when coalesce(ts.total_transactions, 0) >= 5 then 'LIGHT'
        else 'MINIMAL'
    end as activity_level,
    
    -- Fee Burden Analysis
    case 
        when ts.total_transaction_volume > 0 then
            round(coalesce(fs.total_fee_amount, 0) / ts.total_transaction_volume * 100, 4)
        else 0
    end as fee_to_volume_ratio,
    
    current_timestamp as last_updated

from transaction_summary ts
full outer join deposit_summary ds on ts.account_id = ds.account_id
full outer join withdrawal_summary ws on coalesce(ts.account_id, ds.account_id) = ws.account_id
full outer join transfer_combined tc on coalesce(ts.account_id, ds.account_id, ws.account_id) = tc.account_id
full outer join payment_summary ps on coalesce(ts.account_id, ds.account_id, ws.account_id, tc.account_id) = ps.account_id
full outer join fee_summary fs on coalesce(ts.account_id, ds.account_id, ws.account_id, tc.account_id, ps.account_id) = fs.account_id 
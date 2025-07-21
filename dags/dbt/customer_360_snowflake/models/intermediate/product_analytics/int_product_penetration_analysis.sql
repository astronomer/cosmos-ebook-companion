{{ config(
    materialized='view',
    tags=['silver', 'intermediate', 'product_analytics', 'penetration']
) }}

/*
    Product Penetration Analysis – Snowflake-compatible

    Integrates product usage, segmentation, campaign response, and channel activity
    into a unified model for cross-sell and adoption analysis.
*/

with product_usage_summary as (
    select
        customer_id,
        count(distinct product_id) as total_products_used,
        sum(usage_count) as total_usage_events,
        avg(usage_count) as avg_usage_per_product,
        max(usage_date) as last_product_usage_date,
        min(usage_date) as first_product_usage_date,
        count(case when product_id in ('CHCK001', 'SAVE001') then 1 end) as digital_banking_products,
        count(case when product_id = 'LOAN001' then 1 end) as lending_products,
        count(case when product_id = 'INVT001' then 1 end) as investment_products,
        count(case when product_id = 'CD001' then 1 end) as insurance_products,
        sum(case when product_id in ('CHCK001', 'SAVE001') then usage_count end) as digital_banking_usage,
        sum(case when product_id = 'LOAN001' then usage_count end) as lending_usage,
        sum(case when product_id = 'INVT001' then usage_count end) as investment_usage,
        sum(case when product_id = 'CD001' then usage_count end) as insurance_usage
    from {{ ref('stg_product_usage') }}
    group by customer_id
),

customer_segments_ranked as (
    select
        customer_id,
        segment_name as current_segment,
        segment_date as current_segment_date,
        row_number() over (partition by customer_id order by segment_date desc) as rn
    from {{ ref('stg_customer_segments') }}
),

customer_segments_current as (
    select
        customer_id,
        current_segment,
        current_segment_date
    from customer_segments_ranked
    where rn = 1
),

marketing_campaign_response as (
    select
        customer_id,
        count(*) as total_campaigns_targeted,
        0 as campaigns_opened,
        count(case when response_status = 'ENGAGED' then 1 end) as campaigns_clicked,
        count(case when response_status = 'CONVERTED' then 1 end) as campaigns_converted,
        count(case when response_status = 'NO_RESPONSE' then 1 end) as campaigns_opted_out,
        max(campaign_date) as last_campaign_response_date,
        count(case when campaign_type = 'EMAIL_PROMOTION' then 1 end) as cross_sell_campaigns,
        count(case when campaign_type = 'PHONE_CALL' then 1 end) as retention_campaigns,
        count(case when campaign_type in ('DIRECT_MAIL', 'DIGITAL_ADS') then 1 end) as acquisition_campaigns,
        count(case when campaign_type = 'EMAIL_PROMOTION' and response_status = 'CONVERTED' then 1 end) as cross_sell_conversions,
        count(case when response_status = 'CONVERTED' then 1 end) as avg_conversion_value
    from {{ ref('stg_marketing_campaigns') }}
    group by customer_id
),

channel_usage_summary as (
    select
        customer_id,
        count(distinct channel_type) as total_channels_used,
        sum(usage_count) as total_channel_usage,
        count(case when channel_type in ('ONLINE', 'MOBILE') then 1 end) as digital_channels_used,
        count(case when channel_type in ('BRANCH', 'ATM', 'PHONE') then 1 end) as physical_channels_used,
        sum(case when channel_type in ('ONLINE', 'MOBILE') then usage_count end) as digital_channel_usage,
        sum(case when channel_type in ('BRANCH', 'ATM', 'PHONE') then usage_count end) as physical_channel_usage,
        max(usage_date) as last_channel_usage_date
    from {{ ref('stg_channel_usage') }}
    group by customer_id
)

select
    coalesce(
        pus.customer_id,
        csc.customer_id,
        mcr.customer_id,
        cus.customer_id
    ) as customer_id,

    -- Product Usage
    coalesce(pus.total_products_used, 0) as total_products_used,
    coalesce(pus.total_usage_events, 0) as total_usage_events,
    coalesce(pus.avg_usage_per_product, 0) as avg_usage_per_product,
    pus.first_product_usage_date,
    pus.last_product_usage_date,

    -- Product Categories
    coalesce(pus.digital_banking_products, 0) as digital_banking_products,
    coalesce(pus.lending_products, 0) as lending_products,
    coalesce(pus.investment_products, 0) as investment_products,
    coalesce(pus.insurance_products, 0) as insurance_products,

    -- Usage by Category
    coalesce(pus.digital_banking_usage, 0) as digital_banking_usage,
    coalesce(pus.lending_usage, 0) as lending_usage,
    coalesce(pus.investment_usage, 0) as investment_usage,
    coalesce(pus.insurance_usage, 0) as insurance_usage,

    -- Segment Info
    csc.current_segment,
    csc.current_segment_date,

    -- Marketing Campaigns
    coalesce(mcr.total_campaigns_targeted, 0) as total_campaigns_targeted,
    coalesce(mcr.campaigns_opened, 0) as campaigns_opened,
    coalesce(mcr.campaigns_clicked, 0) as campaigns_clicked,
    coalesce(mcr.campaigns_converted, 0) as campaigns_converted,
    coalesce(mcr.campaigns_opted_out, 0) as campaigns_opted_out,
    mcr.last_campaign_response_date,

    coalesce(mcr.cross_sell_campaigns, 0) as cross_sell_campaigns,
    coalesce(mcr.cross_sell_conversions, 0) as cross_sell_conversions,
    coalesce(mcr.retention_campaigns, 0) as retention_campaigns,
    coalesce(mcr.avg_conversion_value, 0) as avg_conversion_value,

    -- Channel Usage
    coalesce(cus.total_channels_used, 0) as total_channels_used,
    coalesce(cus.digital_channels_used, 0) as digital_channels_used,
    coalesce(cus.physical_channels_used, 0) as physical_channels_used,
    coalesce(cus.digital_channel_usage, 0) as digital_channel_usage,
    coalesce(cus.physical_channel_usage, 0) as physical_channel_usage,

    -- Penetration Score
    least(100, greatest(0,
        (coalesce(pus.digital_banking_products, 0) * 15) +
        (coalesce(pus.lending_products, 0) * 25) +
        (coalesce(pus.investment_products, 0) * 30) +
        (coalesce(pus.insurance_products, 0) * 20) +
        (case when pus.total_products_used >= 5 then 10 else 0 end)
    )) as product_penetration_score,

    -- Digital Adoption
    case 
        when pus.digital_banking_products >= 5 then 'DIGITAL_NATIVE'
        when pus.digital_banking_products >= 3 then 'DIGITAL_ADOPTER'
        when pus.digital_banking_products >= 1 then 'DIGITAL_BEGINNER'
        else 'NON_DIGITAL'
    end as digital_adoption_level,

    -- Campaign Responsiveness
    case 
        when mcr.total_campaigns_targeted = 0 then 'UNTARGETED'
        when cast(mcr.campaigns_converted as float) / mcr.total_campaigns_targeted > 0.1 then 'HIGHLY_RESPONSIVE'
        when cast(mcr.campaigns_clicked as float) / mcr.total_campaigns_targeted > 0.05 then 'MODERATELY_RESPONSIVE'
        when cast(mcr.campaigns_opened as float) / mcr.total_campaigns_targeted > 0.02 then 'LIGHTLY_RESPONSIVE'
        else 'UNRESPONSIVE'
    end as marketing_responsiveness,

    -- Channel Preference
    case 
        when cus.digital_channel_usage > cus.physical_channel_usage * 3 then 'DIGITAL_FIRST'
        when cus.digital_channel_usage > cus.physical_channel_usage then 'DIGITAL_PREFERRED'
        when cus.physical_channel_usage > cus.digital_channel_usage then 'BRANCH_PREFERRED'
        when cus.total_channels_used > 0 then 'OMNI_CHANNEL'
        else 'MINIMAL_ENGAGEMENT'
    end as channel_preference,

    -- Specialization
    case 
        when pus.investment_usage > pus.digital_banking_usage + pus.lending_usage + pus.insurance_usage then 'INVESTMENT_FOCUSED'
        when pus.lending_usage > pus.digital_banking_usage + pus.investment_usage + pus.insurance_usage then 'LENDING_FOCUSED'
        when pus.insurance_usage > pus.digital_banking_usage + pus.lending_usage + pus.investment_usage then 'INSURANCE_FOCUSED'
        when pus.digital_banking_usage > 0 then 'BANKING_FOCUSED'
        else 'PRODUCT_EXPLORER'
    end as product_specialization,

    -- Cross-Sell Opportunity
    least(100, greatest(0,
        (case when pus.digital_banking_products = 0 then 20 else 0 end) +
        (case when pus.lending_products = 0 and csc.current_segment in ('PREMIUM', 'MASS_AFFLUENT') then 25 else 0 end) +
        (case when pus.investment_products = 0 and csc.current_segment in ('PREMIUM', 'MASS_AFFLUENT', 'EMERGING_AFFLUENT') then 30 else 0 end) +
        (case when pus.insurance_products = 0 then 15 else 0 end) +
        (case when mcr.cross_sell_conversions > 0 then 10 else 0 end)
    )) as cross_sell_opportunity_score,

    -- Stickiness Score
    case 
        when pus.total_usage_events > 0 then
            least(100, greatest(0,
                (pus.total_products_used * 10) +
                least(50, pus.total_usage_events) +
                (case 
                    when pus.first_product_usage_date is not null then 
                        least(30, datediff(day, pus.first_product_usage_date, current_date) / 30)
                    else 0
                end)
            ))
        else 0
    end as product_stickiness_score,

    -- Next Best Product
    case 
        when pus.investment_products = 0 and csc.current_segment in ('PREMIUM', 'MASS_AFFLUENT') then 'INVESTMENT_ADVISORY'
        when pus.lending_products = 0 and pus.digital_banking_products > 0 then 'PERSONAL_LOAN'
        when pus.insurance_products = 0 and pus.total_products_used > 0 then 'INSURANCE_BUNDLE'
        when pus.digital_banking_products = 0 then 'MOBILE_BANKING'
        when pus.total_products_used >= 2 then 'PREMIUM_SERVICES'
        else 'BASIC_PRODUCTS'
    end as next_best_product,

    current_timestamp() as last_updated

from product_usage_summary pus
full outer join customer_segments_current csc on pus.customer_id = csc.customer_id
full outer join marketing_campaign_response mcr on coalesce(pus.customer_id, csc.customer_id) = mcr.customer_id
full outer join channel_usage_summary cus on coalesce(pus.customer_id, csc.customer_id, mcr.customer_id) = cus.customer_id
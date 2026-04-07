{{ config(
    materialized='view',
    tags=['silver', 'intermediate', 'product_analytics', 'penetration']
) }}

/*
    Product Penetration Analysis
    
    Analyzes product adoption patterns across customer segments:
    - Product usage patterns (stg_product_usage)
    - Customer segmentation (stg_customer_segments)
    - Marketing campaign responses (stg_marketing_campaigns)
    - Channel usage patterns (stg_channel_usage)
    
    Provides insights for product strategy and cross-selling opportunities.
*/

with product_usage_summary as (
    select
        customer_id,
        count(distinct product_type) as total_products_used,
        sum(usage_frequency) as total_usage_events,
        avg(usage_frequency) as avg_usage_per_product,
        max(usage_date) as last_product_usage_date,
        min(usage_date) as first_product_usage_date,
        count(case when product_type in ('CHECKING', 'SAVINGS', 'MOBILE_BANKING') then 1 end) as digital_banking_products,
        count(case when product_type in ('MORTGAGE', 'LOANS') then 1 end) as lending_products,
        count(case when product_type = 'INVESTMENTS' then 1 end) as investment_products,
        count(case when product_type = 'INSURANCE' then 1 end) as insurance_products,
        sum(case when product_type in ('CHECKING', 'SAVINGS', 'MOBILE_BANKING') then usage_frequency end) as digital_banking_usage,
        sum(case when product_type in ('MORTGAGE', 'LOANS') then usage_frequency end) as lending_usage,
        sum(case when product_type = 'INVESTMENTS' then usage_frequency end) as investment_usage,
        sum(case when product_type = 'INSURANCE' then usage_frequency end) as insurance_usage
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
        count(case when response_type = 'OPENED' then 1 end) as campaigns_opened,
        count(case when response_type = 'CLICKED' then 1 end) as campaigns_clicked,
        count(case when response_type = 'CONVERTED' then 1 end) as campaigns_converted,
        count(case when response_type = 'NO_RESPONSE' then 1 end) as campaigns_opted_out,
        max(sent_date) as last_campaign_response_date,
        count(case when campaign_type like '%OFFER%' then 1 end) as cross_sell_campaigns,
        count(case when campaign_type = 'DIGITAL_BANKING' then 1 end) as retention_campaigns,
        count(case when campaign_type in ('CREDIT_CARD_OFFER', 'SAVINGS_PROMOTION') then 1 end) as acquisition_campaigns,
        count(case when campaign_type like '%OFFER%' and response_type = 'CONVERTED' then 1 end) as cross_sell_conversions,
        count(case when response_type = 'CONVERTED' and resulted_in_sale then 1 end) as avg_conversion_value
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
    -- Customer identifier
    coalesce(
        pus.customer_id,
        csc.customer_id,
        mcr.customer_id,
        cus.customer_id
    ) as customer_id,
    
    -- Product Usage Metrics
    coalesce(pus.total_products_used, 0) as total_products_used,
    coalesce(pus.total_usage_events, 0) as total_usage_events,
    coalesce(pus.avg_usage_per_product, 0) as avg_usage_per_product,
    pus.first_product_usage_date,
    pus.last_product_usage_date,
    
    -- Product Category Adoption
    coalesce(pus.digital_banking_products, 0) as digital_banking_products,
    coalesce(pus.lending_products, 0) as lending_products,
    coalesce(pus.investment_products, 0) as investment_products,
    coalesce(pus.insurance_products, 0) as insurance_products,
    
    -- Product Category Usage Intensity
    coalesce(pus.digital_banking_usage, 0) as digital_banking_usage,
    coalesce(pus.lending_usage, 0) as lending_usage,
    coalesce(pus.investment_usage, 0) as investment_usage,
    coalesce(pus.insurance_usage, 0) as insurance_usage,
    
    -- Customer Segmentation
    csc.current_segment,
    csc.current_segment_date,
    
    -- Marketing Campaign Performance
    coalesce(mcr.total_campaigns_targeted, 0) as total_campaigns_targeted,
    coalesce(mcr.campaigns_opened, 0) as campaigns_opened,
    coalesce(mcr.campaigns_clicked, 0) as campaigns_clicked,
    coalesce(mcr.campaigns_converted, 0) as campaigns_converted,
    coalesce(mcr.campaigns_opted_out, 0) as campaigns_opted_out,
    mcr.last_campaign_response_date,
    
    -- Campaign Type Performance
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
    
    -- Product Penetration Metrics
    
    -- Cross-Product Penetration Score (0-100)
    least(100, greatest(0,
        (coalesce(pus.digital_banking_products, 0) * 15) +
        (coalesce(pus.lending_products, 0) * 25) +
        (coalesce(pus.investment_products, 0) * 30) +
        (coalesce(pus.insurance_products, 0) * 20) +
        (case when pus.total_products_used >= 5 then 10 else 0 end)
    )) as product_penetration_score,
    
    -- Digital Product Adoption Level
    case 
        when pus.digital_banking_products >= 5 then 'DIGITAL_NATIVE'
        when pus.digital_banking_products >= 3 then 'DIGITAL_ADOPTER'
        when pus.digital_banking_products >= 1 then 'DIGITAL_BEGINNER'
        else 'NON_DIGITAL'
    end as digital_adoption_level,
    
    -- Marketing Responsiveness
    case 
        when mcr.total_campaigns_targeted = 0 then 'UNTARGETED'
        when mcr.campaigns_converted::numeric / mcr.total_campaigns_targeted > 0.1 then 'HIGHLY_RESPONSIVE'
        when mcr.campaigns_clicked::numeric / mcr.total_campaigns_targeted > 0.05 then 'MODERATELY_RESPONSIVE'
        when mcr.campaigns_opened::numeric / mcr.total_campaigns_targeted > 0.02 then 'LIGHTLY_RESPONSIVE'
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
    
    -- Product Category Specialization
    case 
        when pus.investment_usage > pus.digital_banking_usage + pus.lending_usage + pus.insurance_usage then 'INVESTMENT_FOCUSED'
        when pus.lending_usage > pus.digital_banking_usage + pus.investment_usage + pus.insurance_usage then 'LENDING_FOCUSED'
        when pus.insurance_usage > pus.digital_banking_usage + pus.lending_usage + pus.investment_usage then 'INSURANCE_FOCUSED'
        when pus.digital_banking_usage > 0 then 'BANKING_FOCUSED'
        else 'PRODUCT_EXPLORER'
    end as product_specialization,
    
    -- Cross-Sell Opportunity Score (0-100)
    least(100, greatest(0,
        (case when pus.digital_banking_products = 0 then 20 else 0 end) +
        (case when pus.lending_products = 0 and csc.current_segment in ('PREMIUM', 'MASS_AFFLUENT') then 25 else 0 end) +
        (case when pus.investment_products = 0 and csc.current_segment in ('PREMIUM', 'MASS_AFFLUENT', 'EMERGING_AFFLUENT') then 30 else 0 end) +
        (case when pus.insurance_products = 0 then 15 else 0 end) +
        (case when mcr.cross_sell_conversions > 0 then 10 else 0 end)
    )) as cross_sell_opportunity_score,
    
    -- Product Stickiness Score (0-100)
    case 
        when pus.total_usage_events > 0 then
            least(100, greatest(0,
                (pus.total_products_used * 10) +
                (least(50, pus.total_usage_events)) +
                (case when pus.first_product_usage_date is not null then 
                    least(30, extract(days from current_date - pus.first_product_usage_date) / 30)
                else 0 end)
            ))
        else 0
    end as product_stickiness_score,
    
    -- Next Best Product Recommendation
    case 
        when pus.investment_products = 0 and csc.current_segment in ('PREMIUM', 'MASS_AFFLUENT') then 'INVESTMENT_ADVISORY'
        when pus.lending_products = 0 and pus.digital_banking_products > 0 then 'PERSONAL_LOAN'
        when pus.insurance_products = 0 and pus.total_products_used > 0 then 'INSURANCE_BUNDLE'
        when pus.digital_banking_products = 0 then 'MOBILE_BANKING'
        when pus.total_products_used >= 2 then 'PREMIUM_SERVICES'
        else 'BASIC_PRODUCTS'
    end as next_best_product,
    
    current_timestamp as last_updated

from product_usage_summary pus
full outer join customer_segments_current csc on pus.customer_id = csc.customer_id
full outer join marketing_campaign_response mcr on coalesce(pus.customer_id, csc.customer_id) = mcr.customer_id
full outer join channel_usage_summary cus on coalesce(pus.customer_id, csc.customer_id, mcr.customer_id) = cus.customer_id 
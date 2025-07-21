{{ config(
    materialized='view',
    tags=['bronze', 'staging', 'customers', 'addresses']
) }}

/*
    Staging model for customer address information
    
    Handles address standardization, geocoding, and geographic
    classification for risk and marketing purposes.
*/

with mock_addresses as (
    select
        -- Link to customers
        series_value as address_id,
        case 
            when series_value <= {{ var('num_customers') }} then series_value  -- Primary addresses
            else (series_value % {{ var('num_customers') }}) + 1  -- Some customers have multiple addresses
        end as customer_id,
        
        -- Address type
        case 
            when series_value <= {{ var('num_customers') }} then 'PRIMARY'
            when series_value % 4 = 1 then 'MAILING'
            when series_value % 4 = 2 then 'BUSINESS'
            else 'PREVIOUS'
        end as address_type,
        
        -- Street address
        case 
            when series_value % 5 = 1 then (100 + (series_value % 9900))::text || ' Main Street'
            when series_value % 5 = 2 then (200 + (series_value % 9800))::text || ' Oak Avenue'
            when series_value % 5 = 3 then (300 + (series_value % 9700))::text || ' Pine Road'
            when series_value % 5 = 4 then (400 + (series_value % 9600))::text || ' Elm Drive'
            else (500 + (series_value % 9500))::text || ' Maple Lane'
        end as street_address,
        
        -- Unit/Apartment
        case 
            when series_value % 8 = 1 then 'Apt ' || ((series_value % 50) + 1)::text
            when series_value % 8 = 2 then 'Unit ' || ((series_value % 20) + 1)::text
            else null
        end as unit_number,
        
        -- City
        case 
            when series_value % 10 = 1 then 'New York'
            when series_value % 10 = 2 then 'Los Angeles'
            when series_value % 10 = 3 then 'Chicago'
            when series_value % 10 = 4 then 'Houston'
            when series_value % 10 = 5 then 'Phoenix'
            when series_value % 10 = 6 then 'Philadelphia'
            when series_value % 10 = 7 then 'San Antonio'
            when series_value % 10 = 8 then 'San Diego'
            when series_value % 10 = 9 then 'Dallas'
            else 'Austin'
        end as city,
        
        -- State mapping
        case 
            when series_value % 10 = 1 then 'NY'
            when series_value % 10 = 2 then 'CA'
            when series_value % 10 = 3 then 'IL'
            when series_value % 10 = 4 then 'TX'
            when series_value % 10 = 5 then 'AZ'
            when series_value % 10 = 6 then 'PA'
            when series_value % 10 = 7 then 'TX'
            when series_value % 10 = 8 then 'CA'
            when series_value % 10 = 9 then 'TX'
            else 'TX'
        end as state_code,
        
        -- ZIP codes
        case 
            when series_value % 10 = 1 then (10000 + (series_value % 1000))::text
            when series_value % 10 = 2 then (90000 + (series_value % 1000))::text
            when series_value % 10 = 3 then (60000 + (series_value % 1000))::text
            when series_value % 10 = 4 then (77000 + (series_value % 1000))::text
            when series_value % 10 = 5 then (85000 + (series_value % 1000))::text
            when series_value % 10 = 6 then (19000 + (series_value % 1000))::text
            when series_value % 10 = 7 then (78000 + (series_value % 1000))::text
            when series_value % 10 = 8 then (92000 + (series_value % 1000))::text
            when series_value % 10 = 9 then (75000 + (series_value % 1000))::text
            else (73000 + (series_value % 1000))::text
        end as postal_code,
        
        -- Address status
        case 
            when series_value % 20 = 1 then 'INVALID'
            when series_value % 15 = 1 then 'UNVERIFIED'
            else 'VERIFIED'
        end as address_status,
        
        -- Dates
        '2020-01-01'::date + (series_value * interval '1 day') as address_since,
        case 
            when series_value % 10 = 1 then '2023-01-01'::date + (series_value * interval '3 days')
            else null
        end as address_until,
        
        -- Address validation
        case when series_value % 15 != 1 then true else false end as is_deliverable,
        current_timestamp as last_updated
        
    from generate_series(1, ({{ var('num_customers') }} * {{ var('num_addresses_multiplier') }})::int) as series_value
),

base_addresses as (
    select
        address_id,
        customer_id,
        upper(address_type) as address_type,
        
        -- Standardized address components
        initcap(street_address) as street_address,
        case when unit_number is not null then initcap(unit_number) else null end as unit_number,
        initcap(city) as city,
        upper(state_code) as state_code,
        postal_code,
        
        -- Full formatted address
        concat_ws(', ',
            concat_ws(' ', street_address, unit_number),
            city,
            concat(state_code, ' ', postal_code)
        ) as full_address,
        
        -- Address quality indicators
        upper(address_status) as address_status,
        is_deliverable,
        
        -- Date information
        address_since,
        address_until,
        case 
            when address_until is null then true 
            else false 
        end as is_current,
        
        coalesce(
            date_part('year', age(coalesce(address_until, current_date), address_since))::int,
            0
        ) as years_at_address,
        
        last_updated,
        current_timestamp as dbt_created_at
        
    from mock_addresses
    where address_status != 'INVALID'
),

enriched_addresses as (
    select
        ba.*,
        
        -- Geographic enrichment
        gr.state_name,
        gr.region_name,
        gr.market_type,
        gr.cost_of_living_index,
        gr.population_density,
        
        -- Regional risk assessment
        case 
            when gr.cost_of_living_index > 130 then 'High Cost Area'
            when gr.cost_of_living_index > 110 then 'Medium Cost Area'
            else 'Low Cost Area'
        end as cost_of_living_category,
        
        -- Address stability indicators
        case 
            when ba.years_at_address >= 5 then 'Stable'
            when ba.years_at_address >= 2 then 'Moderate'
            else 'New'
        end as address_stability,
        
        -- Delivery and service flags
        case 
            when ba.is_deliverable and ba.address_status = 'VERIFIED' then true 
            else false 
        end as service_deliverable,
        
        case 
            when gr.population_density = 'High' then true 
            else false 
        end as urban_area,
        
        -- Address scoring for fraud detection
        case 
            when ba.address_status = 'VERIFIED' and ba.years_at_address >= 2 then 90
            when ba.address_status = 'VERIFIED' and ba.years_at_address >= 1 then 75
            when ba.address_status = 'VERIFIED' then 60
            when ba.address_status = 'UNVERIFIED' then 40
            else 20
        end as address_quality_score
        
    from base_addresses ba
    left join {{ ref('geographic_regions') }} gr on ba.state_code = gr.state_code
)

select * from enriched_addresses 
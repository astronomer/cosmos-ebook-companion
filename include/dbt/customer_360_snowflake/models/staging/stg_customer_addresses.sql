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
        ROW_NUMBER() OVER (ORDER BY NULL) as address_id,
        case 
            when ROW_NUMBER() OVER (ORDER BY NULL) <= {{ var('num_customers') }} then ROW_NUMBER() OVER (ORDER BY NULL)  -- Primary addresses
            else (ROW_NUMBER() OVER (ORDER BY NULL) % {{ var('num_customers') }}) + 1  -- Some customers have multiple addresses
        end as customer_id,
        
        -- Address type
        case 
            when ROW_NUMBER() OVER (ORDER BY NULL) <= {{ var('num_customers') }} then 'PRIMARY'
            when ROW_NUMBER() OVER (ORDER BY NULL) % 4 = 1 then 'MAILING'
            when ROW_NUMBER() OVER (ORDER BY NULL) % 4 = 2 then 'BUSINESS'
            else 'PREVIOUS'
        end as address_type,
        
        -- Street address
        case 
            when ROW_NUMBER() OVER (ORDER BY NULL) % 5 = 1 then (100 + (ROW_NUMBER() OVER (ORDER BY NULL) % 9900))::string || ' Main Street'
            when ROW_NUMBER() OVER (ORDER BY NULL) % 5 = 2 then (200 + (ROW_NUMBER() OVER (ORDER BY NULL) % 9800))::string || ' Oak Avenue'
            when ROW_NUMBER() OVER (ORDER BY NULL) % 5 = 3 then (300 + (ROW_NUMBER() OVER (ORDER BY NULL) % 9700))::string || ' Pine Road'
            when ROW_NUMBER() OVER (ORDER BY NULL) % 5 = 4 then (400 + (ROW_NUMBER() OVER (ORDER BY NULL) % 9600))::string || ' Elm Drive'
            else (500 + (ROW_NUMBER() OVER (ORDER BY NULL) % 9500))::string || ' Maple Lane'
        end as street_address,
        
        -- Unit/Apartment
        case 
            when ROW_NUMBER() OVER (ORDER BY NULL) % 8 = 1 then 'Apt ' || ((ROW_NUMBER() OVER (ORDER BY NULL) % 50) + 1)::string
            when ROW_NUMBER() OVER (ORDER BY NULL) % 8 = 2 then 'Unit ' || ((ROW_NUMBER() OVER (ORDER BY NULL) % 20) + 1)::string
            else null
        end as unit_number,
        
        -- City
        case 
            when ROW_NUMBER() OVER (ORDER BY NULL) % 10 = 1 then 'New York'
            when ROW_NUMBER() OVER (ORDER BY NULL) % 10 = 2 then 'Los Angeles'
            when ROW_NUMBER() OVER (ORDER BY NULL) % 10 = 3 then 'Chicago'
            when ROW_NUMBER() OVER (ORDER BY NULL) % 10 = 4 then 'Houston'
            when ROW_NUMBER() OVER (ORDER BY NULL) % 10 = 5 then 'Phoenix'
            when ROW_NUMBER() OVER (ORDER BY NULL) % 10 = 6 then 'Philadelphia'
            when ROW_NUMBER() OVER (ORDER BY NULL) % 10 = 7 then 'San Antonio'
            when ROW_NUMBER() OVER (ORDER BY NULL) % 10 = 8 then 'San Diego'
            when ROW_NUMBER() OVER (ORDER BY NULL) % 10 = 9 then 'Dallas'
            else 'Austin'
        end as city,
        
        -- State mapping
        case 
            when ROW_NUMBER() OVER (ORDER BY NULL) % 10 = 1 then 'NY'
            when ROW_NUMBER() OVER (ORDER BY NULL) % 10 = 2 then 'CA'
            when ROW_NUMBER() OVER (ORDER BY NULL) % 10 = 3 then 'IL'
            when ROW_NUMBER() OVER (ORDER BY NULL) % 10 = 4 then 'TX'
            when ROW_NUMBER() OVER (ORDER BY NULL) % 10 = 5 then 'AZ'
            when ROW_NUMBER() OVER (ORDER BY NULL) % 10 = 6 then 'PA'
            when ROW_NUMBER() OVER (ORDER BY NULL) % 10 = 7 then 'TX'
            when ROW_NUMBER() OVER (ORDER BY NULL) % 10 = 8 then 'CA'
            when ROW_NUMBER() OVER (ORDER BY NULL) % 10 = 9 then 'TX'
            else 'TX'
        end as state_code,
        
        -- ZIP codes
        case 
            when ROW_NUMBER() OVER (ORDER BY NULL) % 10 = 1 then (10000 + (ROW_NUMBER() OVER (ORDER BY NULL) % 1000))::string
            when ROW_NUMBER() OVER (ORDER BY NULL) % 10 = 2 then (90000 + (ROW_NUMBER() OVER (ORDER BY NULL) % 1000))::string
            when ROW_NUMBER() OVER (ORDER BY NULL) % 10 = 3 then (60000 + (ROW_NUMBER() OVER (ORDER BY NULL) % 1000))::string
            when ROW_NUMBER() OVER (ORDER BY NULL) % 10 = 4 then (77000 + (ROW_NUMBER() OVER (ORDER BY NULL) % 1000))::string
            when ROW_NUMBER() OVER (ORDER BY NULL) % 10 = 5 then (85000 + (ROW_NUMBER() OVER (ORDER BY NULL) % 1000))::string
            when ROW_NUMBER() OVER (ORDER BY NULL) % 10 = 6 then (19000 + (ROW_NUMBER() OVER (ORDER BY NULL) % 1000))::string
            when ROW_NUMBER() OVER (ORDER BY NULL) % 10 = 7 then (78000 + (ROW_NUMBER() OVER (ORDER BY NULL) % 1000))::string
            when ROW_NUMBER() OVER (ORDER BY NULL) % 10 = 8 then (92000 + (ROW_NUMBER() OVER (ORDER BY NULL) % 1000))::string
            when ROW_NUMBER() OVER (ORDER BY NULL) % 10 = 9 then (75000 + (ROW_NUMBER() OVER (ORDER BY NULL) % 1000))::string
            else (73000 + (ROW_NUMBER() OVER (ORDER BY NULL) % 1000))::string
        end as postal_code,
        
        -- Address status
        case 
            when ROW_NUMBER() OVER (ORDER BY NULL) % 20 = 1 then 'INVALID'
            when ROW_NUMBER() OVER (ORDER BY NULL) % 15 = 1 then 'UNVERIFIED'
            else 'VERIFIED'
        end as address_status,
        
        -- Dates
        DATEADD(day, ROW_NUMBER() OVER (ORDER BY NULL) * 1, '2020-01-01') as address_since,
        case 
            when ROW_NUMBER() OVER (ORDER BY NULL) % 10 = 1 then DATEADD(day, ROW_NUMBER() OVER (ORDER BY NULL) * 3, '2023-01-01')
            else null
        end as address_until,
        
        -- Address validation
        case when ROW_NUMBER() OVER (ORDER BY NULL) % 15 != 1 then true else false end as is_deliverable,
        current_timestamp as last_updated
        
    from TABLE(GENERATOR(ROWCOUNT => ({{ var('num_customers') }} * {{ var('num_addresses_multiplier') }})::int))
),

base_addresses as (
    select
        address_id,
        customer_id,
        UPPER(address_type) as address_type,
        
        -- Standardized address components
        INITCAP(street_address) as street_address,
        case when unit_number is not null then INITCAP(unit_number) else null end as unit_number,
        INITCAP(city) as city,
        UPPER(state_code) as state_code,
        postal_code,
        
        -- Full formatted address
        concat_ws(', ',
            concat_ws(' ', street_address, unit_number),
            city,
            CONCAT(state_code, ' ', postal_code)
        ) as full_address,
        
        -- Address quality indicators
        UPPER(address_status) as address_status,
        is_deliverable,
        
        -- Date information
        address_since,
        address_until,
        case 
            when address_until is null then true 
            else false 
        end as is_current,
        
        coalesce(
            DATEDIFF(year, address_since, coalesce(address_until, current_date)),
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
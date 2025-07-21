# Customer 360 dbt Project

This dbt project demonstrates a comprehensive Customer 360 data pipeline with advanced patterns and best practices.

## 🚀 Load Testing Configuration

The project uses **dbt variables** to control data volume for performance testing:

```yaml
# dbt_project.yml
vars:
  num_customers: 10                # Base number of customers
  num_addresses_multiplier: 1.5    # 15 addresses (some customers have multiple)
  num_phones_multiplier: 1.7       # 17 phone numbers  
  num_accounts_multiplier: 2.4     # 24 accounts (most customers have multiple)
```

### Override for Load Testing:

**Command Line:**
```bash
# Small test run
dbt run --vars '{"num_customers": 5}'

# Medium load test  
dbt run --vars '{"num_customers": 100}'

# Large load test
dbt run --vars '{"num_customers": 1000, "num_accounts_multiplier": 3.0}'
```

**Environment Variable:**
```bash
# Set via Airflow or environment
export DBT_NUM_CUSTOMERS=50
dbt run --vars '{"num_customers": '${DBT_NUM_CUSTOMERS}'}'
```

**Profile-specific:**
```yaml
# profiles.yml
customer_360:
  outputs:
    dev:
      # ... connection info
      vars:
        num_customers: 10
    staging:
      # ... connection info  
      vars:
        num_customers: 100
    prod:
      # ... connection info
      vars:
        num_customers: 1000
```

This approach scales linearly:
- **10 customers** = ~40 total records (great for development)
- **100 customers** = ~400 total records (good for CI/CD testing)  
- **1000 customers** = ~4000 total records (realistic load testing)

## Project Structure

```
customer_360/
├── dbt_project.yml          # Project configuration with advanced settings
├── models/
│   ├── staging/             # Raw data cleaning and standardization
│   │   └── stg_customers.sql
│   ├── intermediate/        # Business logic transformations (TODO)
│   ├── marts/              # Analytics-ready dimensional models (TODO)
│   └── analytics/          # Aggregated metrics and KPIs (TODO)
├── macros/                 # Reusable SQL macros (TODO)
├── tests/                  # Custom data quality tests (TODO)
├── snapshots/              # Slowly changing dimensions (TODO)
├── seeds/                  # Reference data (TODO)
└── analyses/               # Ad-hoc analyses (TODO)
```

## Architectural Layers

### 1. Staging Layer (`staging/`)
- **Purpose**: Clean and standardize raw data from source systems
- **Materialization**: Views (for performance and storage efficiency)
- **Current Models**: `stg_customers` - Basic customer data cleaning
- **Tags**: `staging`, `customers`

### 2. Intermediate Layer (`intermediate/`)
- **Purpose**: Apply business logic and complex transformations
- **Materialization**: Views (intermediate calculations)
- **Planned Models**: Customer deduplication, data enrichment
- **Tags**: `intermediate`

### 3. Marts Layer (`marts/`)
- **Purpose**: Analytics-ready dimensional models
- **Materialization**: Tables (for query performance)
- **Planned Models**: `dim_customers`, `fct_customer_events`
- **Tags**: `marts`, `core`

### 4. Analytics Layer (`analytics/`)
- **Purpose**: Aggregated metrics and KPIs
- **Materialization**: Tables (for dashboard performance)
- **Planned Models**: Customer lifetime value, segmentation, churn prediction
- **Tags**: `analytics`, `reporting`

## Advanced Features

### Variables
- `start_date`: Controls historical data processing
- `high_value_threshold`: Customer segmentation threshold
- `active_days_threshold`: Activity-based customer classification

### Testing Strategy
- **Source Tests**: Data quality checks on raw data
- **Model Tests**: Business logic validation
- **Custom Tests**: Domain-specific validation rules
- **Store Failures**: Failed tests stored for investigation

### Snapshots
- **Strategy**: Timestamp-based SCD Type 2
- **Target Schema**: `snapshots`
- **Use Cases**: Customer profile changes over time

### Tags and Selection
- **Layer-based**: Run models by architectural layer
- **Domain-based**: Run models by business domain
- **Criticality**: Different retry strategies by importance

## Usage Examples

### Run by Layer
```bash
# Run only staging models
dbt run --select tag:staging

# Run intermediate and marts layers
dbt run --select tag:intermediate tag:marts
```

### Run by Domain
```bash
# Run customer-related models
dbt run --select tag:customers

# Run analytics models
dbt run --select tag:analytics
```

### Testing
```bash
# Run all tests
dbt test

# Run tests for specific layer
dbt test --select tag:staging
```

## Development Roadmap

### Phase 1: Foundation (Current)
- [x] Basic project structure
- [x] Staging layer with customer data
- [x] Comprehensive testing framework
- [x] Documentation
- [x] Load testing configuration

### Phase 2: Enrichment (Next)
- [ ] Customer deduplication logic
- [ ] Data quality scoring
- [ ] External data enrichment
- [ ] Intermediate transformations

### Phase 3: Analytics (Future)
- [ ] Customer lifetime value models
- [ ] Segmentation and scoring
- [ ] Churn prediction
- [ ] Retention analysis

### Phase 4: Advanced Features (Future)
- [ ] Incremental processing
- [ ] Real-time streaming integration
- [ ] Machine learning features
- [ ] Advanced visualization

## Integration with Cosmos

This project is designed to work seamlessly with Airflow Cosmos, demonstrating:
- **Multi-layer DAG structure**: Each layer as a separate DbtTaskGroup
- **Advanced error handling**: Layer-specific retry strategies
- **Data quality validation**: Automated pipeline health checks
- **Observability**: Comprehensive logging and monitoring
- **Scalability**: Patterns for handling large-scale data

## Best Practices Demonstrated

1. **Modular Architecture**: Clear separation of concerns
2. **Configuration Management**: Environment-specific settings
3. **Testing Strategy**: Comprehensive data quality framework
4. **Documentation**: Extensive model and column documentation
5. **Performance Optimization**: Appropriate materializations
6. **Error Handling**: Graceful failure handling
7. **Monitoring**: Data freshness and quality checks
8. **Load Testing**: Configurable data volumes

This project serves as a reference implementation for production-ready dbt projects with Cosmos orchestration. 
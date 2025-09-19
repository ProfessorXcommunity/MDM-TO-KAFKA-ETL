# Banking Data Loader (SBDL) - Spark to Kafka Pipeline

## Overview
A scalable PySpark ETL pipeline designed to process banking data (accounts, parties, addresses) and stream processed records to Apache Kafka. The system transforms relational banking data into JSON event messages following a contract-based schema for downstream consumption.

## Architecture

### Core Components
- **DataLoader**: Handles CSV/Hive data ingestion with configurable schemas for accounts, parties, and addresses
- **ConfigLoader**: Environment-based configuration management for LOCAL/QA/PROD deployments
- **Utils**: Spark session management with optimized configurations
- **Logger**: Custom Log4j integration for comprehensive monitoring
- **Transformation**: Business logic for converting banking data to structured JSON contracts

### Data Flow
```
CSV/Hive Sources → PySpark Processing → JSON Transformation → Kafka Streaming
```

## Features

### ✅ Multi-Environment Support
- **LOCAL**: Development with CSV sources, reduced memory footprint
- **QA/PROD**: Hive integration with production-scale configurations

### ✅ Data Processing
- Schema-validated CSV ingestion
- Runtime filtering with configurable predicates
- Complex nested JSON generation with party relations
- Comprehensive data transformations for banking contracts

### ✅ Quality Assurance
- Complete pytest suite with fixtures
- DataFrame equality testing using Chispa
- Mock data generation and validation
- Schema enforcement and type checking

### ✅ Kafka Integration
- Environment-specific topic routing
- Structured event publishing with headers
- JSON message serialization with versioning

## Configuration

### Environment Settings (`sbdl.conf`)
```ini
[LOCAL]
enable.hive = false
account.filter = active_ind = 1
kafka.topic = sbdl_kafka_cloud

[PROD]
enable.hive = true  
account.filter = active_ind = 1
kafka.topic = sbdl_kafka
```

### Spark Configuration (`spark.conf`)
- Memory allocation: 1G (LOCAL) to 4G (PROD)
- Shuffle partitions: 5 (LOCAL) to 1000 (PROD)
- Adaptive query execution disabled for consistency

## Data Schema

### Input Sources
- **Accounts**: Customer contracts with legal titles, tax IDs, branch codes
- **Parties**: Relationship mapping between accounts and entities
- **Addresses**: Geographic information linked to party IDs

### Output Format
Structured JSON events with event headers, keys, and nested payloads containing:
- Contract identifiers and metadata
- Party relationships with addresses
- Temporal information and operational flags

## Testing

Comprehensive test coverage includes:
- Configuration validation
- Data loading verification  
- Schema compliance testing
- Transformation logic validation
- End-to-end pipeline testing

## Getting Started

### Prerequisites
- Apache Spark 4.0.0
- Python 3.8+
- Kafka cluster access
- Optional: Hive metastore (for production)

### Installation
```bash
# Install dependencies
pip install pyspark pytest chispa

# Configure environments
cp conf/sbdl.conf.template conf/sbdl.conf
cp conf/spark.conf.template conf/spark.conf

# Run tests
pytest test_pytest_sbdl.py -v
```

### Usage
```python
from lib.Utils import get_spark_session
from lib.DataLoader import read_accounts
from lib.Transformation import get_contract

# Initialize Spark session
spark = get_spark_session("LOCAL")

# Load and process data
accounts_df = read_accounts(spark, "LOCAL", False, None)
contract_df = get_contract(accounts_df)

# Stream to Kafka
contract_df.write.format("kafka").save()
```

## Development

### Adding New Data Sources
1. Define schema in `DataLoader.py`
2. Implement reader function with environment support
3. Add configuration filters in `sbdl.conf`
4. Create corresponding test fixtures

### Environment Deployment
- **LOCAL**: CSV-based development with minimal resources
- **QA**: Hive integration with moderate scaling
- **PROD**: Full production deployment with optimized configurations

## Monitoring

The pipeline includes comprehensive logging at multiple levels:
- INFO: Processing milestones and data counts
- WARN: Data quality issues and configuration warnings  
- ERROR: Pipeline failures and data integrity issues
- DEBUG: Detailed execution flow for troubleshooting

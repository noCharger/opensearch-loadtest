# OpenSearch PPL Load Test Framework

## Setup

1. Install dependencies:
   ```bash
   ./scripts/install.sh
   ```

2. Load sample data (run once):
   ```bash
   PYTHONPATH=. python scripts/load_sample_data.py
   ```

3. Run load tests:
   ```bash
   # Basic load test with hardcoded queries
   python run_load_test.py
   
   # Load test using queries from queries/ directory
   python run_queries_load_test.py
   
   # Advanced ramp-up load test
   python run_advanced_ramp_test.py
   
   # Query group-based load test
   python run_group_load_test.py
   
   # Production load test for 10TB dataset
   python run_production_load_test_safe.py --duration 3600 --ramp-step 5 --profile conservative
   ```

## Project Structure

```
├── src/
│   ├── loadtest/          # Core load testing framework
│   │   ├── config.py       # Configuration classes
│   │   ├── load_tester.py  # Main load tester
│   │   └── metrics.py      # Metrics collection
│   └── utils/              # Utility modules
│       ├── wal_logger.py   # WAL logging
│       ├── observability.py # Real-time monitoring
│       ├── query_loader.py # Query loading utilities
│       └── ramp_builder.py # Ramp-up pattern builders
├── queries/                # PPL query files (.ppl)
├── scripts/                # Setup and utility scripts
├── tests/                  # Test examples
└── logs/                   # Generated log files
```

## New Features

### Query Loading
- **Automatic Query Loading**: Load all `.ppl` files from `queries/` directory
- **Selective Query Loading**: Load specific queries by name
- **Query Directory**: All PPL queries are stored in the `queries/` folder

### Ramp-Up Concurrency
- **Linear Ramp**: Gradually increase load from start to end value
- **Exponential Ramp**: Exponentially increase load for stress testing
- **QPS Ramp**: Ramp up queries per second over time
- **Concurrency Ramp**: Ramp up concurrent requests over time

### Query Groups
- **Big 5 Categorization**: Queries automatically grouped into 5 essential areas
- **Group Concurrency Tracking**: Real-time monitoring of concurrent requests per group
- **Group-Based Load Configuration**: Configure different load patterns per group

### Production Load Testing
- **10TB Dataset Optimized**: Conservative concurrency limits for large datasets
- **Real-World Usage Ratios**: Based on production analytics showing 46% aggregation, 21% math functions
- **Configurable Ramp-Up**: Default 5-minute intervals, customizable
- **Enhanced Monitoring**: Node stats reported every 30 seconds
- **CPU Protection**: Auto-pause when CPU usage exceeds 90%, resume when it drops below 85%
- **Load Profiles**: Conservative and moderate profiles matching actual usage patterns

### Usage Examples

```bash
# Production load test (conservative profile, 1 hour, 5-min ramp steps)
python run_production_load_test.py --duration 3600 --ramp-step 5 --profile conservative

# Production test with custom settings
python run_production_load_test.py --host prod-cluster --port 443 --ssl \
  --duration 7200 --ramp-step 10 --profile moderate
```

```python
# Configure load by query group
for query in queries:
    if query.query_group == QueryGroup.SORTING:
        query.target_concurrency = RampBuilder.linear_concurrency_ramp(
            start=1, end=2, steps=3, step_duration=300  # 5 minutes
        )

# Monitor shows group concurrency:
# [30s] Group Concurrency: sorting: 2 | text_querying: 1 | range_queries: 3
```

## Framework assumes test data is already loaded in OpenSearch indices.
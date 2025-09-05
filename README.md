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
   
   # DSL queries load test with index creation
   python run_dsl_load_test.py --dsl-file queries/dsl_queries.json --create-index --profile conservative
   
   # DSL queries load test with production planning
   python run_dsl_load_test.py --dsl-file queries/dsl_queries.json --duration 3600 --ramp-step 5 --profile moderate
   
   # DSL queries load test with warmup phase
   python run_dsl_load_test.py --dsl-file queries/dsl_queries.json --warmup --warmup-duration 600 --profile conservative
   
   # Load test with warmup using PPL queries
   python run_warmup_test.py --warmup-duration 600 --duration 300 --index "big5*"
   
   # Mixed PPL and DSL queries load test
   python run_mixed_load_test.py --ppl-qps 1.0 --dsl-qps 0.5
   
   # Advanced ramp-up load test
   python run_advanced_ramp_test.py
   
   # Query group-based load test
   python run_group_load_test.py
   
   # Production load test for 10TB dataset
   python run_production_load_test_safe.py --host opense-clust-rslsbropig6j-9ee3cfa8c6137509.elb.us-west-2.amazonaws.com --port 443 --ssl --username admin --password CalciteLoadTest@123 --duration 3600 --ramp-step 10 --profile concurrent --index "big5*"
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
├── queries/                # PPL query files (.ppl) and DSL queries (.json)
├── scripts/                # Setup and utility scripts
├── tests/                  # Test examples
└── logs/                   # Generated log files
```

## New Features

### Query Loading
- **Automatic Query Loading**: Load all `.ppl` files from `queries/` directory
- **DSL Query Loading**: Load OpenSearch DSL queries from JSON files
- **Mixed Query Loading**: Load both PPL and DSL queries in the same test
- **Selective Query Loading**: Load specific queries by name
- **Query Directory**: PPL queries (`.ppl`) and DSL queries (`.json`) stored in `queries/` folder

### Ramp-Up Concurrency
- **Linear Ramp**: Gradually increase load from start to end value
- **Exponential Ramp**: Exponentially increase load for stress testing
- **QPS Ramp**: Ramp up queries per second over time
- **Concurrency Ramp**: Ramp up concurrent requests over time
- **Single Group Exponential**: Focus exponential load on one query group (1→64 concurrent)
- **Single Query Exponential**: Focus exponential load on one specific query (1→64 concurrent)
- **Single Group Power-of-2 Ramp**: Focus power-of-2 load on one query group (1→2→4→8→16→32→64)

### Warmup Phase
- **Automatic Warmup**: Run all queries at same concurrency before main test
- **Configurable Duration**: Default 10 minutes, customizable warmup duration
- **Cache Warming**: Prepares OpenSearch caches and JVM for optimal performance
- **Separate Metrics**: Warmup metrics logged separately from main test results

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
- **DSL Query Support**: Full production planning support for OpenSearch DSL queries
- **Index Management**: Automatic index creation with mapping support
- **Single Group/Query Testing**: Exponential ramp testing for individual query groups or queries

### Usage Examples

```bash
# Production load test with warmup against AWS OpenSearch cluster
python run_production_load_test.py --host opense-clust-F0oiBIzi4TAq-6ffabb460181b586.elb.us-west-2.amazonaws.com --port 443 --ssl --username admin --password CalciteLoadTest@123 --duration 3600 --ramp-step 5 --profile conservative --index "big5*" --warmup --warmup-duration 600

# Production load test against AWS OpenSearch cluster
python run_production_load_test.py --host opense-clust-F0oiBIzi4TAq-6ffabb460181b586.elb.us-west-2.amazonaws.com --port 443 --ssl --username admin --password CalciteLoadTest@123 --duration 3600 --ramp-step 5 --profile conservative --index "big5*"

# Safe production test with CPU protection
python run_production_load_test_safe.py --host opense-clust-F0oiBIzi4TAq-6ffabb460181b586.elb.us-west-2.amazonaws.com --port 443 --ssl --username admin --password CalciteLoadTest@123 --duration 3600 --ramp-step 5 --profile conservative --index "big5*"

# DSL queries load test with index creation and production planning
python run_dsl_load_test.py --host localhost --port 9200 --dsl-file queries/dsl_queries.json --create-index --profile conservative --duration 3600

# DSL queries load test with moderate profile
python run_dsl_load_test.py --host localhost --port 9200 --dsl-file queries/dsl_queries.json --profile moderate --ramp-step 10 --duration 7200

# Mixed PPL and DSL load test
python run_mixed_load_test.py --host localhost --port 9200 --ppl-qps 1.0 --dsl-qps 0.5 --duration 300

# DSL queries with concurrent profile (exponential ramp)
python run_dsl_load_test.py --host localhost --port 9200 --dsl-file queries/dsl_queries.json --profile concurrent --ramp-step 10 --duration 7200

# Single DSL query exponential test (match-all only, 1->64 concurrent)
python run_dsl_load_test.py --host localhost --port 9200 --dsl-file queries/dsl_queries.json --profile single_query_exponential --target-query match-all --duration 3600 --ramp-step 10

# DSL queries with warmup phase
python run_dsl_load_test.py --host localhost --port 9200 --dsl-file queries/dsl_queries.json --warmup --warmup-duration 600 --profile conservative

# PPL queries with warmup
python run_warmup_test.py --host localhost --port 9200 --warmup-duration 600 --duration 1800 --index "big5*"
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

# Load mixed PPL and DSL queries
queries = QueryLoader.load_mixed_queries(
    ppl_queries_dir="queries",
    dsl_json_file="queries/dsl_queries.json",
    index_pattern="big5*"
)

# Configure different load patterns for different query types
for query in queries:
    if query.query_type == QueryType.PPL:
        query.target_qps = 2.0
    elif query.query_type == QueryType.DSL:
        query.target_qps = 1.0
```

### DSL Query Format

DSL queries are stored in JSON files using Rally-style format:

```json
[
  {
    "name": "match-all",
    "operation-type": "search",
    "index": "{{index_name | default('big5')}}",
    "body": {
      "query": {
        "match_all": {}
      }
    }
  },
  {
    "name": "term",
    "operation-type": "search",
    "index": "{{index_name | default('big5')}}",
    "request-timeout": 7200,
    "body": {
      "query": {
        "term": {
          "log.file.path": {
            "value": "/var/log/messages/birdknight"
          }
        }
      }
    }
  }
]
```

- `name`: Unique identifier for the query
- `operation-type`: Must be "search" for search operations
- `index`: Target index pattern (supports template variables)
- `body`: OpenSearch DSL query body
- `request-timeout`: Optional timeout in seconds

### PPL Load Test Setup

PPL Load Test implements a step-by-step load testing approach that gradually introduces PPL queries to measure incremental performance impact.

#### Test Configuration

* 5 PPL Queries: composite_terms, desc_sort_timestamp, range, default, term
* Progressive Steps: Each query starts at a different time step
* Concurrency: 10 concurrent requests per active query
* Duration: Configurable (default 25 minutes = 5 steps × 5 minutes each)

#### Execution Pattern

```
Step 1 (0-5min):   composite_terms only                           (10 concurrent)
Step 2 (5-10min):  composite_terms + desc_sort_timestamp          (20 concurrent) 
Step 3 (10-15min): composite_terms + desc_sort_timestamp + range   (30 concurrent)
Step 4 (15-20min): + default                                      (40 concurrent)
Step 5 (20-25min): + term                                         (50 concurrent)
```

#### Optional Warmup

* Warmup Phase: All queries run at 1 concurrency each for cache warming
* Separate Logging: Warmup metrics logged separately from main test data

#### PPL Load Test Commands

```bash
# PPL load test with high concurrency profile
python run_ppl_load_test.py --profile high_concurrency --duration 1500 --ramp-step 5 --index "big5*"

# PPL load test with conservative profile
python run_ppl_load_test.py --profile conservative --duration 3600 --ramp-step 5 --index "big5*"

# Single query group power-of-2 ramp test (sorting only, 1->2->4->8->16->32->64)
python run_ppl_load_test.py --profile single_group_power2 --target-group sorting --duration 3600 --ramp-step 10 --index "big5*"

# PPL load test with warmup phase
python run_ppl_load_test.py --profile conservative --warmup --warmup-duration 600 --index "big5*"

# Production PPL load test against AWS OpenSearch
python run_ppl_load_test.py --host opense-clust-rslsbropig6j-9ee3cfa8c6137509.elb.us-west-2.amazonaws.com --port 443 --ssl --username admin --password CalciteLoadTest@123 --duration 3600 --ramp-step 5 --profile high_concurrency --index "custom-big5*"
```

## Framework assumes test data is already loaded in OpenSearch indices.
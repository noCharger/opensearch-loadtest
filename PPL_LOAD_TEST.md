# PPL Load Test Setup

PPL Load Test implements a production-grade load testing approach that gradually ramps up PPL queries across different query groups to measure performance impact on large datasets.

## Test Configuration

* **5 PPL Query Groups**: range_queries, sorting, terms_aggregation, text_querying, date_histogram
* **Linear Ramp Pattern**: Gradual concurrency increase over 60 minutes
* **Production Profiles**: Conservative, moderate, and high_concurrency options
* **Duration**: Configurable (default 3600 seconds = 60 minutes)

## Execution Pattern

Based on production analytics and query group categorization:

### Conservative Profile (60-minute ramp)
```
=== Target Concurrency Timeline ===
Time        range_queries  sorting  terms_aggregation  text_querying  Total
------------------------------------------------------------------------
0s start              1          1          1              2            5
300s (5min)           1          1          1              2            5
600s (10min)          1          1          1              2            5
900s (15min)          1          1          1              2            5
1200s (20min)         2          2          2              4           10
1500s (25min)         3          3          3              6           15
1800s (30min)         5          5          5             10           25
2100s (35min)         7          7          7             14           35
2400s (40min)        10         10         10             20           50
2700s (45min)        14         14         14             28           70
3000s (50min)        20         20         20             40          100
3300s (55min)        28         28         28             56          140
3600s (60min)        39         39         39             78          195
```

### High Concurrency Profile (60-minute ramp)
```
=== Target Concurrency Timeline ===
Time        composite_terms  desc_sort_timestamp  range  default  term  Total
---------------------------------------------------------------------------
0s start                  1                    1      1        1     1      5
300s (5min)               2                    2      2        2     2     10
600s (10min)              3                    3      3        3     3     15
900s (15min)              4                    4      4        4     4     20
1200s (20min)             5                    5      5        5     5     25
1500s (25min)             6                    6      6        6     6     30
1800s (30min)             7                    7      7        7     7     35
2100s (35min)             8                    8      8        8     8     40
2400s (40min)             9                    9      9        9     9     45
2700s (45min)            10                   10     10       10    10     50
```

## Query Group Distribution

Based on production usage patterns:

* **terms_aggregation**: 46% (STATS, COUNT operations) - Highest load
* **text_querying**: 21% (Basic search operations) - Moderate load  
* **range_queries**: 11% (WHERE clauses) - Moderate load
* **sorting**: 8% (SORT operations) - Lower load
* **date_histogram**: 12% (Time functions) - Moderate load

## Optional Warmup Phase

* **Warmup Duration**: All queries run at 1 concurrency each for cache warming
* **Separate Metrics**: Warmup metrics logged separately from main test data
* **JVM Preparation**: Prepares OpenSearch caches and JVM for optimal performance

## PPL Load Test Commands

### Basic Usage
```bash
# Conservative profile (safe for production)
python run_ppl_load_test.py --profile conservative --duration 3600 --ramp-step 5 --index "big5*"

# High concurrency profile (stress testing)
python run_ppl_load_test.py --profile high_concurrency --duration 3600 --ramp-step 5 --index "big5*"

# With warmup phase
python run_ppl_load_test.py --profile conservative --warmup --warmup-duration 600 --index "big5*"
```

### Production AWS OpenSearch
```bash
# Production PPL load test against AWS OpenSearch cluster
python run_ppl_load_test.py \
  --host opense-clust-rslsbropig6j-9ee3cfa8c6137509.elb.us-west-2.amazonaws.com \
  --port 443 --ssl \
  --username admin --password CalciteLoadTest@123 \
  --duration 3600 --ramp-step 5 \
  --profile conservative \
  --index "custom-big5*"

# High concurrency production test
python run_ppl_load_test.py \
  --host opense-clust-rslsbropig6j-9ee3cfa8c6137509.elb.us-west-2.amazonaws.com \
  --port 443 --ssl \
  --username admin --password CalciteLoadTest@123 \
  --duration 3600 --ramp-step 5 \
  --profile high_concurrency \
  --index "custom-big5*" \
  --warmup --warmup-duration 600
```

## Load Profiles

### Conservative Profile
- **Target**: Production-safe testing
- **Max Concurrency**: ~195 total concurrent requests
- **Ramp Pattern**: Linear increase over 60 minutes
- **Use Case**: Baseline performance measurement

### High Concurrency Profile  
- **Target**: Stress testing capabilities
- **Max Concurrency**: 50 total concurrent requests (10 per query)
- **Ramp Pattern**: Linear increase over 60 minutes
- **Use Case**: Performance limits and bottleneck identification

### Moderate Profile
- **Target**: Balanced load testing
- **Max Concurrency**: ~100 total concurrent requests
- **Ramp Pattern**: Linear increase over 60 minutes
- **Use Case**: Regular performance validation

## Monitoring and Metrics

* **Real-time Monitoring**: Group concurrency tracking every 30 seconds
* **Node Statistics**: CPU, memory, and disk usage monitoring
* **Query Metrics**: Response times, success rates, and error tracking
* **WAL Logging**: Write-ahead logging for all query executions
* **Separate Warmup Logs**: Warmup phase metrics logged independently

## PPL Query Examples

The test uses production-representative PPL queries:

```sql
-- composite_terms: Aggregation heavy
source=big5* | stats count() by log.level, host.name

-- desc_sort_timestamp: Sorting operations  
source=big5* | sort @timestamp desc | head 1000

-- range: Filtering operations
source=big5* | where @timestamp > "2023-01-01" | head 100

-- default: Basic search
source=big5* | head 10

-- term: Specific field matching
source=big5* | where log.level="ERROR" | head 50
```

## Framework Requirements

* Test data must be pre-loaded in OpenSearch indices
* Index pattern should match the `--index` parameter (default: "big5*")
* OpenSearch cluster should have PPL plugin enabled
* Sufficient cluster resources for target concurrency levels
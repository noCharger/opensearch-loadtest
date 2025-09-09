# Single Group Power-of-2 Load Test Setup

## Overview

The `single_group_power2` profile focuses exponential load testing on a single query group while keeping all other groups at zero concurrency. This allows isolated performance analysis of specific query categories.

## 1. Test Parameters

### Query Groups
- **TEXT_QUERYING**: Full-text search and matching operations
- **SORTING**: Sort-based queries and ordering operations  
- **DATE_HISTOGRAM**: Time-based aggregations and histograms
- **RANGE_QUERIES**: Range filtering and boundary queries
- **TERMS_AGGREGATION**: Term-based aggregations and grouping

### Load Configuration
- **Target Group**: Single group receives exponential load
- **Non-Target Groups**: All other groups maintain 0 concurrency
- **Ramp Pattern**: Power-of-2 scaling (1→2→4→8→16→32→64) or Conservative (1→2→4→6→8→10→12...)
- **Step Duration**: Configurable (default 10 minutes per step)
- **Special Handling**: DATE_HISTOGRAM and TERMS_AGGREGATION use conservative ramp due to resource intensity

## 2. Concurrency Timeline

### Standard Groups (TEXT_QUERYING, SORTING, RANGE_QUERIES)
```
Step 1 (0-10min):   Target Group: 1  | Others: 0
Step 2 (10-20min):  Target Group: 2  | Others: 0  
Step 3 (20-30min):  Target Group: 4  | Others: 0
Step 4 (30-40min):  Target Group: 8  | Others: 0
Step 5 (40-50min):  Target Group: 16 | Others: 0
Step 6 (50-60min):  Target Group: 32 | Others: 0
Step 7 (60-70min):  Target Group: 64 | Others: 0
```

### Resource-Intensive Groups (DATE_HISTOGRAM, TERMS_AGGREGATION)
```
Step 1 (0-10min):   Target Group: 1  | Others: 0
Step 2 (10-20min):  Target Group: 2  | Others: 0  
Step 3 (20-30min):  Target Group: 4  | Others: 0
Step 4 (30-40min):  Target Group: 6  | Others: 0
Step 5 (40-50min):  Target Group: 8  | Others: 0
Step 6 (50-60min):  Target Group: 10 | Others: 0
Step 7 (60-70min):  Target Group: 12 | Others: 0
```

### Timeline Display Example
```
[10m] Group Concurrency: sorting: 2 | text_querying: 0 | date_histogram: 0 | range_queries: 0 | terms_aggregation: 0
[20m] Group Concurrency: sorting: 4 | text_querying: 0 | date_histogram: 0 | range_queries: 0 | terms_aggregation: 0
[30m] Group Concurrency: sorting: 8 | text_querying: 0 | date_histogram: 0 | range_queries: 0 | terms_aggregation: 0
```

## 3. Execution Pattern

### PPL Queries
```bash
# Test sorting group with power-of-2 ramp
python run_ppl_load_test.py --profile single_group_power2 --target-group sorting --duration 4200 --ramp-step 10 --index "big5*"

# Test text querying group
python run_ppl_load_test.py --profile single_group_power2 --target-group text_querying --duration 4200 --ramp-step 10 --index "big5*"

# Test date histogram group  
python run_ppl_load_test.py --profile single_group_power2 --target-group date_histogram --duration 4200 --ramp-step 10 --index "big5*"
```

### DSL Queries
```bash
# Test terms aggregation group
python run_dsl_load_test.py --profile single_group_power2 --target-group terms_aggregation --duration 4200 --ramp-step 10

# Test range queries group
python run_dsl_load_test.py --profile single_group_power2 --target-group range_queries --duration 4200 --ramp-step 10
```

### Production Environment
```bash
# Production single group test
python run_ppl_load_test.py \
  --host opense-clust-example.elb.us-west-2.amazonaws.com \
  --port 443 --ssl \
  --username admin --password CalciteLoadTest@123 \
  --profile single_group_power2 \
  --target-group sorting \
  --duration 4200 --ramp-step 10 \
  --index "big5*"
```

## Benefits

- **Isolated Testing**: Focus on single query group performance
- **Exponential Scaling**: Rapid load increase to find breaking points
- **Clear Metrics**: No interference from other query types
- **Resource Analysis**: Identify group-specific resource consumption patterns
#!/bin/bash

# DSL Target Query Load Tests
# Runs individual query tests with power-of-2 ramp pattern

HOST="opense-clust-rslsbropig6j-9ee3cfa8c6137509.elb.us-west-2.amazonaws.com"
PORT="443"
USERNAME="admin"
PASSWORD="CalciteLoadTest@123"
DURATION="7200"  # 2 hours
RAMP_STEP="10"   # 10 minutes
INDEX="custom-big5*"

BASE_CMD="python run_dsl_load_test.py --host $HOST --port $PORT --ssl --username $USERNAME --password $PASSWORD --profile single_query_power2 --duration $DURATION --ramp-step $RAMP_STEP --index $INDEX --dsl-file queries/dsl_queries.json"

echo "Starting DSL Target Query Load Tests..."
echo "Duration: $DURATION seconds (2 hours)"
echo "Ramp Step: $RAMP_STEP minutes"
echo "Pattern: Power-of-2 then linear (1→2→4→8→16→32→64→100→150→200...)"
echo ""

# Test 1: range query
echo "=== Test 1: range query ==="
echo "Command: $BASE_CMD --target-query range"
$BASE_CMD --target-query range
echo ""

# Test 2: match-all query  
echo "=== Test 2: match-all query ==="
echo "Command: $BASE_CMD --target-query match-all"
$BASE_CMD --target-query match-all
echo ""

# Test 3: term query
echo "=== Test 3: term query ==="
echo "Command: $BASE_CMD --target-query term"
$BASE_CMD --target-query term
echo ""

# Test 4: date_histogram_hourly_agg query
echo "=== Test 4: date_histogram_hourly_agg query ==="
echo "Command: $BASE_CMD --target-query date_histogram_hourly_agg"
$BASE_CMD --target-query date_histogram_hourly_agg
echo ""

# Test 5: keyword-terms query
echo "=== Test 5: keyword-terms query ==="
echo "Command: $BASE_CMD --target-query keyword-terms"
$BASE_CMD --target-query keyword-terms
echo ""

echo "All DSL target query tests completed!"
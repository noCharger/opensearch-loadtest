#!/usr/bin/env python3

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from src.loadtest.config import LoadTestConfig, QueryConfig, QueryType, QPSRamp, LoadMode, ConcurrencyRamp
from src.loadtest.load_tester import LoadTester

def main():
    # Define queries
    queries = [
        QueryConfig(
            name="match_all_search",
            query_type=QueryType.DSL,
            query='{"query": {"match_all": {}}}',
            target_qps=5.0,
            index="test-index"
        ),
        QueryConfig(
            name="ppl_basic_search",
            query_type=QueryType.PPL,
            query="source=test-index | head 10",
            target_qps=[
                QPSRamp(qps=1.0, duration_seconds=10),
                QPSRamp(qps=3.0, duration_seconds=10),
                QPSRamp(qps=5.0, duration_seconds=10)
            ]
        ),
        QueryConfig(
            name="dsl_aggregation",
            query_type=QueryType.DSL,
            query='{"aggs": {"avg_value": {"avg": {"field": "value"}}}}',
            target_qps=0.1,
            index="test-index"
        ),
        QueryConfig(
            name="slow_ppl_query",
            query_type=QueryType.PPL,
            query="source=test-index | stats avg(value) by level",
            load_mode=LoadMode.CONCURRENCY,
            target_concurrency=[
                ConcurrencyRamp(concurrency=1, duration_seconds=15),
                ConcurrencyRamp(concurrency=3, duration_seconds=15)
            ]
        )
    ]
    
    # Configure test
    config = LoadTestConfig(
        host="localhost",
        port=9200,
        duration_seconds=30,
        queries=queries,
        # Optional: Export metrics to separate cluster
        metrics_host="opense-clust-f0oibizi4taq-6ffabb460181b586.elb.us-west-2.amazonaws.com",
        metrics_port=443,
        metrics_use_ssl=True,
        metrics_username="admin",
        metrics_password="CalciteLoadTest@123"
    )
    
    # Run load test
    tester = LoadTester(config)
    results = tester.run_test()
    
    # Print results
    print("\n=== Load Test Results ===")
    
    # Overall results
    overall = results['overall']
    print(f"\nOverall:")
    print(f"  Total requests: {overall['total']}")
    print(f"  Successful: {overall['success']}")
    print(f"  Errors: {overall['errors']}")
    print(f"  Success rate: {overall['success_rate']:.2f}%")
    print(f"  Average duration: {overall['avg_duration']:.3f}s")
    print(f"  Actual RPS: {overall['rps']:.2f}")
    
    # Per-query results
    print(f"\nPer-Query Results:")
    for query_name, stats in results.items():
        if query_name != 'overall':
            print(f"  {query_name}:")
            print(f"    Requests: {stats['total']} | Success: {stats['success']} | Errors: {stats['errors']}")
            print(f"    Success rate: {stats['success_rate']:.2f}% | Avg RPS: {stats['rps']:.2f}")
            print(f"    Latency - Avg: {stats['avg_duration']:.3f}s | P90: {stats['p90_duration']:.3f}s | Min: {stats['min_duration']:.3f}s | Max: {stats['max_duration']:.3f}s")
            print(f"    Throughput - Avg: {stats['avg_throughput']:.2f} | Min: {stats['min_throughput']:.2f} | Max: {stats['max_throughput']:.2f}")

if __name__ == "__main__":
    main()
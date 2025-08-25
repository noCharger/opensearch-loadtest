#!/usr/bin/env python3

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from src.loadtest.config import LoadTestConfig, LoadMode
from src.loadtest.load_tester import LoadTester
from src.utils.query_loader import QueryLoader
from src.utils.ramp_builder import RampBuilder

def main():
    # Load all queries from queries directory
    queries = QueryLoader.load_queries_from_directory()
    
    # Configure ramp-up for each query
    for query in queries:
        if "slow" in query.name or "aggregation" in query.name:
            # Use concurrency ramp for complex queries
            query.load_mode = LoadMode.CONCURRENCY
            query.target_concurrency = RampBuilder.linear_concurrency_ramp(
                start=1, end=5, steps=3, step_duration=20
            )
        else:
            # Use QPS ramp for simple queries
            query.load_mode = LoadMode.QPS
            query.target_qps = RampBuilder.linear_qps_ramp(
                start=1.0, end=10.0, steps=4, step_duration=15
            )
    
    # Configure test
    config = LoadTestConfig(
        host="localhost",
        port=9200,
        duration_seconds=60,
        queries=queries[:5]  # Run first 5 queries only
    )
    
    # Run load test
    tester = LoadTester(config)
    results = tester.run_test()
    
    # Print results
    print("\\n=== Load Test Results ===")
    
    overall = results['overall']
    print(f"\\nOverall:")
    print(f"  Total requests: {overall['total']}")
    print(f"  Successful: {overall['success']}")
    print(f"  Errors: {overall['errors']}")
    print(f"  Success rate: {overall['success_rate']:.2f}%")
    print(f"  Average duration: {overall['avg_duration']:.3f}s")
    print(f"  Actual RPS: {overall['rps']:.2f}")
    
    print(f"\\nPer-Query Results:")
    for query_name, stats in results.items():
        if query_name != 'overall':
            print(f"  {query_name}:")
            print(f"    Requests: {stats['total']} | Success: {stats['success']} | Errors: {stats['errors']}")
            print(f"    Success rate: {stats['success_rate']:.2f}% | Avg RPS: {stats['rps']:.2f}")
            print(f"    Latency - Avg: {stats['avg_duration']:.3f}s | P90: {stats['p90_duration']:.3f}s")

if __name__ == "__main__":
    main()
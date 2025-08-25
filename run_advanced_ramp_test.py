#!/usr/bin/env python3

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from src.loadtest.config import LoadTestConfig, LoadMode
from src.loadtest.load_tester import LoadTester
from src.utils.query_loader import QueryLoader
from src.utils.ramp_builder import RampBuilder

def main():
    # Load specific queries
    query_names = ["default", "range", "term", "sort_numeric_asc"]
    queries = QueryLoader.load_specific_queries(query_names)
    
    # Configure different ramp strategies for each query
    queries[0].load_mode = LoadMode.QPS
    queries[0].target_qps = RampBuilder.linear_qps_ramp(
        start=0.5, end=8.0, steps=5, step_duration=12
    )
    
    queries[1].load_mode = LoadMode.CONCURRENCY
    queries[1].target_concurrency = RampBuilder.exponential_concurrency_ramp(
        start=1, end=8, steps=4, step_duration=15
    )
    
    queries[2].load_mode = LoadMode.QPS
    queries[2].target_qps = 5.0  # Constant QPS
    
    queries[3].load_mode = LoadMode.CONCURRENCY
    queries[3].target_concurrency = RampBuilder.linear_concurrency_ramp(
        start=2, end=6, steps=3, step_duration=20
    )
    
    # Configure test
    config = LoadTestConfig(
        host="localhost",
        port=9200,
        duration_seconds=60,
        queries=queries
    )
    
    # Run load test
    tester = LoadTester(config)
    results = tester.run_test()
    
    # Print results
    print("\\n=== Advanced Ramp Load Test Results ===")
    
    overall = results['overall']
    print(f"\\nOverall:")
    print(f"  Total requests: {overall['total']}")
    print(f"  Success rate: {overall['success_rate']:.2f}%")
    print(f"  Average duration: {overall['avg_duration']:.3f}s")
    print(f"  Actual RPS: {overall['rps']:.2f}")

if __name__ == "__main__":
    main()
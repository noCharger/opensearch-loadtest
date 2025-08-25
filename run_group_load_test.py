#!/usr/bin/env python3

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from src.loadtest.config import LoadTestConfig, LoadMode
from src.loadtest.load_tester import LoadTester
from src.utils.query_loader import QueryLoader
from src.utils.ramp_builder import RampBuilder
from src.utils.query_groups import QueryGroup

def main():
    # Load queries and group them
    queries = QueryLoader.load_queries_from_directory()
    
    # Configure different load patterns per group
    for query in queries:
        if query.query_group == QueryGroup.TEXT_QUERYING:
            query.load_mode = LoadMode.QPS
            query.target_qps = 3.0
        elif query.query_group == QueryGroup.SORTING:
            query.load_mode = LoadMode.CONCURRENCY
            query.target_concurrency = RampBuilder.linear_concurrency_ramp(
                start=1, end=4, steps=3, step_duration=20
            )
        elif query.query_group == QueryGroup.DATE_HISTOGRAM:
            query.load_mode = LoadMode.CONCURRENCY
            query.target_concurrency = 2
        elif query.query_group == QueryGroup.RANGE_QUERIES:
            query.load_mode = LoadMode.QPS
            query.target_qps = RampBuilder.linear_qps_ramp(
                start=1.0, end=5.0, steps=4, step_duration=15
            )
        elif query.query_group == QueryGroup.TERMS_AGGREGATION:
            query.load_mode = LoadMode.CONCURRENCY
            query.target_concurrency = 1
    
    # Run subset for demo
    config = LoadTestConfig(
        host="localhost",
        port=9200,
        duration_seconds=60,
        queries=queries[:8]
    )
    
    tester = LoadTester(config)
    results = tester.run_test()
    
    print("\\n=== Group Load Test Results ===")
    overall = results['overall']
    print(f"Total requests: {overall['total']} | Success rate: {overall['success_rate']:.2f}%")

if __name__ == "__main__":
    main()
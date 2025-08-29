#!/usr/bin/env python3

import argparse
from src.loadtest.config import LoadTestConfig, QueryConfig, QueryType, LoadMode
from src.loadtest.load_tester import LoadTester
from src.utils.query_loader import QueryLoader

def main():
    parser = argparse.ArgumentParser(description='Run load test with warmup phase')
    parser.add_argument('--host', default='localhost', help='OpenSearch host')
    parser.add_argument('--port', type=int, default=9200, help='OpenSearch port')
    parser.add_argument('--ssl', action='store_true', help='Use SSL')
    parser.add_argument('--username', help='Username for authentication')
    parser.add_argument('--password', help='Password for authentication')
    parser.add_argument('--warmup-duration', type=int, default=600, help='Warmup duration in seconds (default: 600)')
    parser.add_argument('--duration', type=int, default=300, help='Test duration in seconds (default: 300)')
    parser.add_argument('--index', default='big5*', help='Index pattern')
    
    args = parser.parse_args()
    
    # Load queries from queries directory
    queries = QueryLoader.load_ppl_queries("queries", index_pattern=args.index)
    
    if not queries:
        print("No queries found in queries/ directory")
        return
    
    # Configure queries with moderate concurrency
    for query in queries:
        query.load_mode = LoadMode.CONCURRENCY
        query.target_concurrency = 2  # Moderate concurrency for main test
    
    # Create load test configuration with warmup enabled
    config = LoadTestConfig(
        host=args.host,
        port=args.port,
        use_ssl=args.ssl,
        username=args.username,
        password=args.password,
        duration_seconds=args.duration,
        queries=queries,
        index_pattern=args.index,
        warmup_enabled=True,
        warmup_duration_seconds=args.warmup_duration
    )
    
    # Run load test with warmup
    tester = LoadTester(config)
    results = tester.run_test()
    
    # Print results
    print("\n=== Load Test Results ===")
    for query_name, stats in results.items():
        if query_name == "overall":
            continue
        print(f"\n{query_name}:")
        print(f"  Total requests: {stats['total']}")
        print(f"  Success rate: {stats['success_rate']:.1f}%")
        print(f"  Avg latency: {stats['avg_duration_ms']:.1f}ms")
        print(f"  P90 latency: {stats['p90_duration_ms']:.1f}ms")
        print(f"  Throughput: {stats['rps']:.2f} RPS")

if __name__ == "__main__":
    main()
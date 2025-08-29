#!/usr/bin/env python3

import argparse
from src.loadtest.config import LoadTestConfig, LoadMode
from src.loadtest.load_tester import LoadTester
from src.utils.query_loader import QueryLoader
from src.utils.ramp_builder import RampBuilder

def main():
    parser = argparse.ArgumentParser(description='OpenSearch Mixed PPL/DSL Load Test')
    parser.add_argument('--host', default='localhost', help='OpenSearch host')
    parser.add_argument('--port', type=int, default=9200, help='OpenSearch port')
    parser.add_argument('--ssl', action='store_true', help='Use SSL')
    parser.add_argument('--username', help='Username for authentication')
    parser.add_argument('--password', help='Password for authentication')
    parser.add_argument('--duration', type=int, default=60, help='Test duration in seconds')
    parser.add_argument('--index', default='big5*', help='Index pattern to test against')
    parser.add_argument('--ppl-dir', default='queries', help='PPL queries directory')
    parser.add_argument('--dsl-file', default='queries/dsl_queries.json', help='DSL queries JSON file')
    parser.add_argument('--ppl-qps', type=float, default=1.0, help='Target QPS for PPL queries')
    parser.add_argument('--dsl-qps', type=float, default=0.5, help='Target QPS for DSL queries')
    parser.add_argument('--concurrent', action='store_true', help='Use concurrent execution mode')
    
    args = parser.parse_args()
    
    # Load mixed queries
    queries = QueryLoader.load_mixed_queries(args.ppl_dir, args.dsl_file, args.index)
    
    if not queries:
        print("No queries found")
        return
    
    # Configure load for each query based on type
    for query in queries:
        if args.concurrent:
            query.load_mode = LoadMode.CONCURRENCY
            # Use different concurrency levels based on query type
            if query.query_type.value == 'ppl':
                query.target_concurrency = 2
            else:  # DSL
                query.target_concurrency = 1
        else:
            query.load_mode = LoadMode.QPS
            # Use different QPS based on query type
            if query.query_type.value == 'ppl':
                query.target_qps = args.ppl_qps
            else:  # DSL
                query.target_qps = args.dsl_qps
    
    # Create and run load test
    config = LoadTestConfig(
        host=args.host,
        port=args.port,
        use_ssl=args.ssl,
        username=args.username,
        password=args.password,
        duration_seconds=args.duration,
        queries=queries,
        index_pattern=args.index
    )
    
    load_tester = LoadTester(config)
    results = load_tester.run_test()
    
    # Print results grouped by query type
    print("\n=== Load Test Results ===")
    
    ppl_queries = [q for q in queries if q.query_type.value == 'ppl']
    dsl_queries = [q for q in queries if q.query_type.value == 'dsl']
    
    if ppl_queries:
        print("\nPPL Queries:")
        for query in ppl_queries:
            stats = results.get(query.name, {})
            print(f"  {query.name}: {stats.get('success', 0)}/{stats.get('total', 0)} "
                  f"({stats.get('success_rate', 0):.1%}) - {stats.get('rps', 0):.2f} RPS")
    
    if dsl_queries:
        print("\nDSL Queries:")
        for query in dsl_queries:
            stats = results.get(query.name, {})
            print(f"  {query.name}: {stats.get('success', 0)}/{stats.get('total', 0)} "
                  f"({stats.get('success_rate', 0):.1%}) - {stats.get('rps', 0):.2f} RPS")
    
    overall = results.get("overall", {})
    print(f"\nOverall:")
    print(f"  Total: {overall.get('total', 0)}")
    print(f"  Success: {overall.get('success', 0)}")
    print(f"  Errors: {overall.get('errors', 0)}")
    print(f"  Success Rate: {overall.get('success_rate', 0):.1%}")
    print(f"  Avg Duration: {overall.get('avg_duration', 0):.3f}s")
    print(f"  RPS: {overall.get('rps', 0):.2f}")

if __name__ == "__main__":
    main()
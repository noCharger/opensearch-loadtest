#!/usr/bin/env python3

import sys
import os
import argparse
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from src.loadtest.config import LoadTestConfig
from src.loadtest.load_tester import LoadTester
from src.utils.query_loader import QueryLoader
from src.utils.production_config import ProductionLoadConfig

def main():
    parser = argparse.ArgumentParser(description='Production Load Test for 10TB Dataset')
    parser.add_argument('--host', default='localhost', help='OpenSearch host')
    parser.add_argument('--port', type=int, default=9200, help='OpenSearch port')
    parser.add_argument('--duration', type=int, default=3600, help='Test duration in seconds (default: 1 hour)')
    parser.add_argument('--ramp-step', type=int, default=5, help='Ramp step duration in minutes (default: 5)')
    parser.add_argument('--profile', choices=['conservative', 'moderate'], default='conservative', 
                       help='Load profile (default: conservative)')
    parser.add_argument('--ssl', action='store_true', help='Use SSL connection')
    parser.add_argument('--username', help='Username for authentication')
    parser.add_argument('--password', help='Password for authentication')
    parser.add_argument('--index', default='big5*', help='Index pattern (default: big5*)')
    
    args = parser.parse_args()
    
    print(f"=== Production Load Test Configuration ===")
    print(f"Dataset: 10TB")
    print(f"Target: {args.host}:{args.port}")
    print(f"Duration: {args.duration // 60} minutes")
    print(f"Ramp step: {args.ramp_step} minutes")
    print(f"Profile: {args.profile}")
    print(f"Node stats interval: 30 seconds")
    
    # Load all queries with configurable index pattern
    queries = QueryLoader.load_queries_from_directory(index_pattern=args.index)
    
    # Apply production configuration
    if args.profile == 'conservative':
        config_dict = ProductionLoadConfig.get_conservative_ramp_config(args.ramp_step)
    else:
        config_dict = ProductionLoadConfig.get_moderate_ramp_config(args.ramp_step)
    
    ProductionLoadConfig.apply_config_to_queries(queries, config_dict)
    
    # Configure test
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
    
    print(f"\\nLoaded {len(queries)} queries across 5 groups")
    print(f"Index pattern: {args.index}")
    print(f"SSL: {args.ssl}")
    print(f"Authentication: {'Yes' if args.username else 'No'}")
    print("Starting production load test...")
    
    # Run load test
    tester = LoadTester(config)
    results = tester.run_test()
    
    # Print summary results
    print("\\n=== Production Load Test Results ===")
    overall = results['overall']
    print(f"Total requests: {overall['total']}")
    print(f"Success rate: {overall['success_rate']:.2f}%")
    print(f"Average latency: {overall['avg_duration']:.3f}s")
    print(f"Actual RPS: {overall['rps']:.2f}")
    
    # Group summary
    from collections import defaultdict
    group_stats = defaultdict(lambda: {'total': 0, 'success': 0})
    
    for query_name, stats in results.items():
        if query_name != 'overall':
            # Find query group (simplified lookup)
            query = next((q for q in queries if q.name == query_name), None)
            if query and query.query_group:
                group_stats[query.query_group.value]['total'] += stats['total']
                group_stats[query.query_group.value]['success'] += stats['success']
    
    print("\\nGroup Performance:")
    for group, stats in group_stats.items():
        success_rate = (stats['success'] / stats['total'] * 100) if stats['total'] > 0 else 0
        print(f"  {group}: {stats['total']} requests, {success_rate:.1f}% success")

if __name__ == "__main__":
    main()
#!/usr/bin/env python3

import sys
import os
import argparse
import atexit
from pathlib import Path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from src.loadtest.config import LoadTestConfig
from src.loadtest.load_tester import LoadTester
from src.utils.query_loader import QueryLoader
from src.utils.production_config import ProductionLoadConfig

class SafePPLLoadTester:
    """Context manager wrapper for safe PPL load testing"""
    
    def __init__(self, config):
        self.config = config
        self.tester = None
        
    def __enter__(self):
        print("Initializing PPL LoadTester...")
        self.tester = LoadTester(self.config)
        print("PPL LoadTester initialized successfully")
        atexit.register(self._cleanup)
        return self.tester
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        self._cleanup()
        
    def _cleanup(self):
        if self.tester:
            print("Performing cleanup...")
            try:
                self.tester._stop_event.set()
                self.tester.monitor.stop_monitoring()
                self.tester.wal_logger.log("EXECUTION", "CLEANUP")
            except Exception as e:
                print(f"Cleanup warning: {e}")

def main():
    print("Starting PPL Load Test with Production Planning...")
    
    parser = argparse.ArgumentParser(description='PPL Load Test with Production Planning')
    parser.add_argument('--host', default='localhost', help='OpenSearch host')
    parser.add_argument('--port', type=int, default=9200, help='OpenSearch port')
    parser.add_argument('--duration', type=int, default=3600, help='Test duration in seconds')
    parser.add_argument('--ramp-step', type=int, default=5, help='Ramp step duration in minutes')
    parser.add_argument('--profile', choices=['conservative', 'moderate', 'concurrent', 'high_concurrency', 'single_group_exponential', 'single_group_power2'], default='conservative')
    parser.add_argument('--target-group', choices=['terms_aggregation', 'text_querying', 'range_queries', 'sorting', 'date_histogram'], help='Target query group for single_group_exponential/single_group_power2 profile')
    parser.add_argument('--target-query', choices=['composite_terms', 'desc_sort_timestamp', 'range', 'default', 'term'], help='Target query for single query exponential test')
    parser.add_argument('--ssl', action='store_true', help='Use SSL connection')
    parser.add_argument('--username', help='Username for authentication')
    parser.add_argument('--password', help='Password for authentication')
    parser.add_argument('--index', default='big5*', help='Index pattern (default: big5*)')
    parser.add_argument('--warmup', action='store_true', help='Enable warmup phase')
    parser.add_argument('--warmup-duration', type=int, default=600, help='Warmup duration in seconds (default: 600)')
    
    print("Parsing arguments...")
    args = parser.parse_args()
    print(f"Arguments parsed: host={args.host}, port={args.port}, index={args.index}")
    
    # Load specific PPL queries
    ppl_query_names = ["composite_terms", "desc_sort_timestamp", "range", "default", "term"]
    print(f"Loading specific PPL queries: {ppl_query_names}")
    queries = QueryLoader.load_specific_queries(ppl_query_names, "queries", args.index)
    print(f"Loaded {len(queries)} PPL queries")
    
    if not queries:
        print("No PPL queries found in queries/ directory")
        return
    
    print(f"Applying {args.profile} profile configuration...")
    if args.profile == 'high_concurrency':
        config_dict = ProductionLoadConfig.get_ppl_high_concurrency_config(args.ramp_step, args.duration)
        ProductionLoadConfig.apply_ppl_config_to_queries(queries, config_dict)
    elif args.profile == 'concurrent':
        config_dict = ProductionLoadConfig.get_concurrent_ramp_config(args.ramp_step)
        ProductionLoadConfig.apply_config_to_queries(queries, config_dict)
    elif args.profile == 'single_group_exponential':
        if args.target_group:
            from src.utils.query_groups import QueryGroup
            target_group = QueryGroup(args.target_group)
            config_dict = ProductionLoadConfig.get_single_group_exponential_config(target_group, args.ramp_step, args.duration)
            ProductionLoadConfig.apply_single_group_config_to_queries(queries, config_dict, target_group)
        elif args.target_query:
            config_dict = ProductionLoadConfig.get_single_query_exponential_config(args.target_query, args.ramp_step, args.duration)
            ProductionLoadConfig.apply_single_query_config_to_queries(queries, config_dict, args.target_query, args.duration)
        else:
            print("Error: single_group_exponential profile requires --target-group or --target-query")
            return
    elif args.profile == 'single_group_power2':
        if args.target_group:
            from src.utils.query_groups import QueryGroup
            target_group = QueryGroup(args.target_group)
            config_dict = ProductionLoadConfig.get_single_group_power2_ramp_config(target_group, args.ramp_step, args.duration)
            ProductionLoadConfig.apply_single_group_config_to_queries(queries, config_dict, target_group)
        elif args.target_query:
            config_dict = ProductionLoadConfig.get_single_query_power2_ramp_config(args.target_query, args.ramp_step, args.duration)
            ProductionLoadConfig.apply_single_query_config_to_queries(queries, config_dict, args.target_query, args.duration)
        else:
            print("Error: single_group_power2 profile requires --target-group or --target-query")
            return
    else:
        if args.profile == 'conservative':
            config_dict = ProductionLoadConfig.get_conservative_ramp_config(args.ramp_step)
        else:
            config_dict = ProductionLoadConfig.get_moderate_ramp_config(args.ramp_step)
        ProductionLoadConfig.apply_config_to_queries(queries, config_dict)
    print("Configuration applied to queries")
    
    # Configure test
    config = LoadTestConfig(
        host=args.host,
        port=args.port,
        use_ssl=args.ssl,
        username=args.username,
        password=args.password,
        duration_seconds=args.duration,
        queries=queries,
        index_pattern=args.index,
        warmup_enabled=args.warmup,
        warmup_duration_seconds=args.warmup_duration,
        metrics_host=args.host,
        metrics_port=args.port,
        metrics_use_ssl=args.ssl,
        metrics_username=args.username,
        metrics_password=args.password
    )
    
    print("=== PPL Load Test with Production Planning ===")
    if args.profile == 'single_group_exponential':
        if args.target_group:
            print(f"Single Group Exponential Test: {args.target_group} (1->64 concurrent)")
        elif args.target_query:
            print(f"Single Query Exponential Test: {args.target_query} (1->64 concurrent)")
    elif args.profile == 'single_group_power2':
        if args.target_group:
            print(f"Single Group Power-of-2 Ramp Test: {args.target_group} (1->2->4->8->16->32->64)")
        elif args.target_query:
            print(f"Single Query Power-of-2 Ramp Test: {args.target_query} (1->2->4->8->16->32->64)")
    profile_desc = args.profile
    if args.profile in ['single_group_exponential', 'single_group_power2']:
        if args.target_group:
            profile_desc += f" (group: {args.target_group})"
        elif args.target_query:
            profile_desc += f" (query: {args.target_query})"
    print(f"Testing {len(queries)} PPL queries with {profile_desc} profile")
    print("Press Ctrl+C for graceful shutdown")
    
    try:
        with SafePPLLoadTester(config) as tester:
            results = tester.run_test()
            
        # Print results
        print("\n=== PPL Load Test Results ===")
        for query_name, stats in results.items():
            if query_name != "overall":
                print(f"\n{query_name}:")
                print(f"  Total: {stats['total']}")
                print(f"  Success: {stats['success']}")
                print(f"  Errors: {stats['errors']}")
                print(f"  Success Rate: {stats['success_rate']:.1%}")
                print(f"  Avg Duration: {stats['avg_duration']:.3f}s")
                print(f"  RPS: {stats['rps']:.2f}")
        
        overall = results.get("overall", {})
        print(f"\nOverall Results:")
        print(f"  Total requests: {overall.get('total', 0)}")
        print(f"  Success rate: {overall.get('success_rate', 0):.2f}%")
        print(f"  Average RPS: {overall.get('rps', 0):.2f}")
        
    except KeyboardInterrupt:
        print("\nTest interrupted by user")
    except Exception as e:
        print(f"\nTest failed: {e}")
    finally:
        print("Resources cleaned up successfully")

if __name__ == "__main__":
    main()
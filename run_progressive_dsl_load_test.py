#!/usr/bin/env python3

import sys
import os
import argparse
import atexit
from pathlib import Path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from src.loadtest.config import LoadTestConfig, QueryConfig, QueryType, LoadMode, ConcurrencyRamp
from src.loadtest.load_tester import LoadTester
from src.utils.dsl_query_loader import DSLQueryLoader
from src.utils.query_groups import QueryGroup
from scripts.explain import PPLExplainer

class SafeProgressiveDSLLoadTester:
    """Context manager wrapper for safe progressive DSL load testing"""
    
    def __init__(self, config):
        self.config = config
        self.tester = None
        
    def __enter__(self):
        print("Initializing Progressive DSL LoadTester...")
        self.tester = LoadTester(self.config)
        print("Progressive DSL LoadTester initialized successfully")
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

def create_index_if_needed(host, port, ssl, username, password, index_name):
    """Create big5 index using the explainer"""
    print(f"Setting up index: {index_name}")
    
    protocol = "https" if ssl else "http"
    base_url = f"{protocol}://{host}:{port}"
    
    if username and password:
        from urllib.parse import quote
        base_url = f"{protocol}://{quote(username)}:{quote(password)}@{host}:{port}"
    
    project_root = Path(__file__).parent
    mapping_file = project_root / "queries" / "mappings" / "big5_index_mapping.json"
    
    try:
        explainer = PPLExplainer(base_url=base_url)
        explainer.create_index(
            index_name=index_name,
            mapping_file=str(mapping_file) if mapping_file.exists() else None,
            data_file=None,
            batch_size=1000
        )
        print(f"Index {index_name} created successfully")
    except Exception as e:
        print(f"Warning: Failed to create index {index_name}: {e}")
        print("Continuing with existing index...")

def create_progressive_dsl_config(host, port, ssl, username, password, duration, index_pattern, warmup_enabled=False, warmup_duration=600):
    """Create progressive DSL load test configuration"""
    
    # Load DSL queries
    dsl_queries = DSLQueryLoader.load_queries_from_json("queries/dsl_queries.json", index_pattern)
    
    # Define query order for progressive loading
    query_order = ["match-all", "term", "range-numeric", "composite-terms", "desc_sort_timestamp"]
    
    # Create progressive concurrency ramps
    step_duration = duration // len(query_order)  # Equal time per step
    
    queries = []
    for i, query_name in enumerate(query_order):
        # Find the DSL query
        dsl_query = next((q for q in dsl_queries if q.name == query_name), None)
        if not dsl_query:
            continue
            
        # Calculate when this query starts (step i)
        start_step = i
        
        # Build concurrency ramp: 0 for initial steps, then 2 concurrent
        concurrency_ramp = []
        
        # Steps before this query starts: 0 concurrency
        for step in range(start_step):
            concurrency_ramp.append(ConcurrencyRamp(concurrency=0, duration_seconds=step_duration))
        
        # Steps when this query runs: 10 concurrent
        remaining_steps = len(query_order) - start_step
        for step in range(remaining_steps):
            concurrency_ramp.append(ConcurrencyRamp(concurrency=10, duration_seconds=step_duration))
        
        # Determine query group
        if query_name == "desc_sort_timestamp":
            query_group = QueryGroup.SORTING
        else:
            query_group = QueryGroup.TEXT_QUERYING
        
        query_config = QueryConfig(
            name=query_name,
            query=dsl_query.query,
            query_type=QueryType.DSL,
            load_mode=LoadMode.CONCURRENCY,
            target_concurrency=concurrency_ramp,
            index=index_pattern,
            query_group=query_group
        )
        queries.append(query_config)
    
    return LoadTestConfig(
        host=host,
        port=port,
        use_ssl=ssl,
        username=username,
        password=password,
        duration_seconds=duration,
        queries=queries,
        index_pattern=index_pattern,
        metrics_host=host,
        metrics_port=port,
        metrics_use_ssl=ssl,
        metrics_username=username,
        metrics_password=password,
        warmup_enabled=warmup_enabled,
        warmup_duration_seconds=warmup_duration
    )

def main():
    print("Starting Progressive DSL Load Test...")
    
    parser = argparse.ArgumentParser(description='Progressive DSL Load Test')
    parser.add_argument('--host', default='localhost', help='OpenSearch host')
    parser.add_argument('--port', default=9200, type=int, help='OpenSearch port')
    parser.add_argument('--ssl', action='store_true', help='Use SSL')
    parser.add_argument('--username', help='OpenSearch username')
    parser.add_argument('--password', help='OpenSearch password')
    parser.add_argument('--duration', default=1500, type=int, help='Test duration in seconds (default: 25 minutes)')
    parser.add_argument('--index', default='big5*', help='Index pattern')
    parser.add_argument('--create-index', action='store_true', help='Create index before running test')
    parser.add_argument('--warmup', action='store_true', help='Enable warmup phase')
    parser.add_argument('--warmup-duration', type=int, default=600, help='Warmup duration in seconds (default: 600)')
    
    args = parser.parse_args()
    
    # Create index if requested
    if args.create_index:
        create_index_if_needed(args.host, args.port, args.ssl, args.username, args.password, args.index.replace('*', ''))
    
    print("=== Progressive DSL Load Test ===")
    if args.warmup:
        print(f"Warmup: {args.warmup_duration} seconds ({args.warmup_duration//60} minutes)")
    print(f"Duration: {args.duration} seconds ({args.duration//60} minutes)")
    print(f"Step duration: {args.duration//5} seconds ({args.duration//300} minutes per step)")
    print("\nProgressive Plan:")
    if args.warmup:
        print(f"Warmup ({args.warmup_duration//60}min): All queries at 1 concurrent each")
    print("Step 1 (0-6min): match-all only (10 concurrent)")
    print("Step 2 (6-12min): match-all + term (20 total concurrent)")
    print("Step 3 (12-18min): match-all + term + range-numeric (30 total concurrent)")
    print("Step 4 (18-24min): match-all + term + range-numeric + composite-terms (40 total concurrent)")
    print("Step 5 (24-30min): All queries running (50 total concurrent)")
    if args.warmup:
        print(f"\nTotal test time: {args.warmup_duration + args.duration} seconds ({(args.warmup_duration + args.duration)//60} minutes)")
    print("Press Ctrl+C for graceful shutdown")
    
    config = create_progressive_dsl_config(
        host=args.host,
        port=args.port,
        ssl=args.ssl,
        username=args.username,
        password=args.password,
        duration=args.duration,
        index_pattern=args.index,
        warmup_enabled=args.warmup,
        warmup_duration=args.warmup_duration
    )
    
    try:
        with SafeProgressiveDSLLoadTester(config) as tester:
            results = tester.run_test()
            
        print("\n=== Progressive DSL Load Test Results ===")
        for query_name, stats in results.items():
            if query_name != "overall":
                print(f"\n{query_name}:")
                print(f"  Total: {stats['total']}")
                print(f"  Success: {stats['success']}")
                print(f"  Errors: {stats['errors']}")
                print(f"  Success Rate: {stats['success_rate']:.1f}%")
                print(f"  Avg Duration: {stats['avg_duration_ms']:.1f}ms")
                print(f"  P90 Duration: {stats['p90_duration_ms']:.1f}ms")
        
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
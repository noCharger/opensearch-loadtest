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
from scripts.explain import PPLExplainer

class SafeDSLLoadTester:
    """Context manager wrapper for safe DSL load testing"""
    
    def __init__(self, config):
        self.config = config
        self.tester = None
        
    def __enter__(self):
        print("Initializing DSL LoadTester...")
        self.tester = LoadTester(self.config)
        print("DSL LoadTester initialized successfully")
        # Register cleanup function
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
    
    # Construct base URL
    protocol = "https" if ssl else "http"
    base_url = f"{protocol}://{host}:{port}"
    
    # Add authentication if provided
    if username and password:
        from urllib.parse import quote
        base_url = f"{protocol}://{quote(username)}:{quote(password)}@{host}:{port}"
    
    # Paths for mapping and data files
    project_root = Path(__file__).parent
    mapping_file = project_root / "queries" / "mappings" / "big5_index_mapping.json"
    
    try:
        explainer = PPLExplainer(base_url=base_url)
        explainer.create_index(
            index_name=index_name,
            mapping_file=str(mapping_file) if mapping_file.exists() else None,
            data_file=None,  # No data loading for load test
            batch_size=1000
        )
        print(f"Index {index_name} created successfully")
    except Exception as e:
        print(f"Warning: Failed to create index {index_name}: {e}")
        print("Continuing with existing index...")

def main():
    print("Starting DSL Load Test with Production Planning...")
    
    parser = argparse.ArgumentParser(description='DSL Load Test with Production Planning')
    parser.add_argument('--host', default='localhost', help='OpenSearch host')
    parser.add_argument('--port', type=int, default=9200, help='OpenSearch port')
    parser.add_argument('--duration', type=int, default=3600, help='Test duration in seconds')
    parser.add_argument('--ramp-step', type=int, default=5, help='Ramp step duration in minutes')
    parser.add_argument('--profile', choices=['conservative', 'moderate', 'concurrent', 'high_concurrency'], default='conservative')
    parser.add_argument('--ssl', action='store_true', help='Use SSL connection')
    parser.add_argument('--username', help='Username for authentication')
    parser.add_argument('--password', help='Password for authentication')
    parser.add_argument('--index', default='big5', help='Index name (default: big5)')
    parser.add_argument('--dsl-file', default='queries/dsl_queries.json', help='DSL queries JSON file')
    parser.add_argument('--create-index', action='store_true', help='Create index before running test')
    parser.add_argument('--warmup', action='store_true', help='Enable warmup phase')
    parser.add_argument('--warmup-duration', type=int, default=600, help='Warmup duration in seconds (default: 600)')
    
    print("Parsing arguments...")
    args = parser.parse_args()
    print(f"Arguments parsed: host={args.host}, port={args.port}, index={args.index}")
    
    # Create index if requested
    if args.create_index:
        create_index_if_needed(args.host, args.port, args.ssl, args.username, args.password, args.index)
    
    # Load DSL queries
    print(f"Loading DSL queries from: {args.dsl_file}")
    queries = QueryLoader.load_dsl_queries(args.dsl_file, args.index)
    print(f"Loaded {len(queries)} DSL queries")
    
    if not queries:
        print(f"No DSL queries found in {args.dsl_file}")
        return
    
    print(f"Applying {args.profile} profile configuration...")
    if args.profile == 'high_concurrency':
        config_dict = ProductionLoadConfig.get_dsl_high_concurrency_config(args.ramp_step)
        ProductionLoadConfig.apply_dsl_config_to_queries(queries, config_dict)
    else:
        if args.profile == 'conservative':
            config_dict = ProductionLoadConfig.get_conservative_ramp_config(args.ramp_step)
        elif args.profile == 'concurrent':
            config_dict = ProductionLoadConfig.get_concurrent_ramp_config(args.ramp_step)
        else:
            config_dict = ProductionLoadConfig.get_moderate_ramp_config(args.ramp_step)
        ProductionLoadConfig.apply_config_to_queries(queries, config_dict)
    print("Configuration applied to queries")
    print("Creating LoadTestConfig...")
    
    # Configure test with metrics export to same cluster
    config = LoadTestConfig(
        host=args.host,
        port=args.port,
        use_ssl=args.ssl,
        username=args.username,
        password=args.password,
        duration_seconds=args.duration,
        queries=queries,
        index_pattern=args.index,
        # Warmup configuration
        warmup_enabled=args.warmup,
        warmup_duration_seconds=args.warmup_duration,
        # Export metrics to same cluster by default
        metrics_host=args.host,
        metrics_port=args.port,
        metrics_use_ssl=args.ssl,
        metrics_username=args.username,
        metrics_password=args.password
    )
    
    print("=== DSL Load Test with Production Planning ===")
    print(f"Testing {len(queries)} DSL queries with {args.profile} profile")
    print("Press Ctrl+C for graceful shutdown")
    print("Creating SafeDSLLoadTester...")
    
    try:
        print("Creating SafeDSLLoadTester context manager...")
        with SafeDSLLoadTester(config) as tester:
            print("SafeDSLLoadTester created, starting test...")
            results = tester.run_test()
            
        # Print results
        print("\n=== DSL Load Test Results ===")
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
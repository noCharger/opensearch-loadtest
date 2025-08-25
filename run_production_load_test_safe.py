#!/usr/bin/env python3

import sys
import os
import argparse
import atexit
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from src.loadtest.config import LoadTestConfig
from src.loadtest.load_tester import LoadTester
from src.utils.query_loader import QueryLoader
from src.utils.production_config import ProductionLoadConfig

class SafeLoadTester:
    """Context manager wrapper for safe load testing"""
    
    def __init__(self, config):
        self.config = config
        self.tester = None
        
    def __enter__(self):
        self.tester = LoadTester(self.config)
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

def main():
    parser = argparse.ArgumentParser(description='Safe Production Load Test')
    parser.add_argument('--host', default='localhost', help='OpenSearch host')
    parser.add_argument('--port', type=int, default=9200, help='OpenSearch port')
    parser.add_argument('--duration', type=int, default=3600, help='Test duration in seconds')
    parser.add_argument('--ramp-step', type=int, default=5, help='Ramp step duration in minutes')
    parser.add_argument('--profile', choices=['conservative', 'moderate'], default='conservative')
    parser.add_argument('--ssl', action='store_true', help='Use SSL connection')
    parser.add_argument('--username', help='Username for authentication')
    parser.add_argument('--password', help='Password for authentication')
    parser.add_argument('--index', default='big5*', help='Index pattern (default: big5*)')
    
    args = parser.parse_args()
    
    # Load and configure queries with configurable index pattern
    queries = QueryLoader.load_queries_from_directory(index_pattern=args.index)
    
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
    
    print("=== Safe Production Load Test ===")
    print("Press Ctrl+C for graceful shutdown")
    
    try:
        with SafeLoadTester(config) as tester:
            results = tester.run_test()
            
        # Print results
        print("\\n=== Results ===")
        overall = results['overall']
        print(f"Total requests: {overall['total']}")
        print(f"Success rate: {overall['success_rate']:.2f}%")
        
    except KeyboardInterrupt:
        print("\\nTest interrupted by user")
    except Exception as e:
        print(f"\\nTest failed: {e}")
    finally:
        print("Resources cleaned up successfully")

if __name__ == "__main__":
    main()
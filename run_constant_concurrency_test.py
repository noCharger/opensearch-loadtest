#!/usr/bin/env python3
import time
import argparse
from src.loadtest.config import LoadTestConfig
from src.loadtest.load_tester import LoadTester
from src.utils.concurrency_controller import ConcurrencyController
from src.utils.query_groups import QueryGroup
from src.utils.query_loader import QueryLoader
from src.utils.observability import ObservabilityMonitor

def main():
    parser = argparse.ArgumentParser(description='Constant Concurrency Load Test')
    parser.add_argument('--host', default='localhost', help='OpenSearch host')
    parser.add_argument('--port', default=9200, type=int, help='OpenSearch port')
    parser.add_argument('--username', required=True, help='Username')
    parser.add_argument('--password', required=True, help='Password')
    parser.add_argument('--ssl', action='store_true', help='Use SSL')
    parser.add_argument('--duration', default=300, type=int, help='Test duration in seconds')
    parser.add_argument('--index', default='big5*', help='Index pattern')
    
    args = parser.parse_args()
    
    # Load queries
    loader = QueryLoader()
    queries = loader.load_queries_from_directory('queries')
    
    if not queries:
        print("No queries found in queries/ directory")
        return
    
    # Group queries by type
    query_groups = {}
    for query in queries:
        group = query.query_group
        if group not in query_groups:
            query_groups[group] = []
        query_groups[group].append(query.query.replace('source=big5*', f'source={args.index}'))
    
    print(f"Loaded {len(queries)} queries in {len(query_groups)} groups")
    
    # Create load tester
    config = LoadTestConfig(
        host=args.host,
        port=args.port,
        username=args.username,
        password=args.password,
        use_ssl=args.ssl,
        duration=args.duration,
        queries=[]  # Not used in this mode
    )
    
    load_tester = LoadTester(config)
    monitor = ObservabilityMonitor()
    
    # Create concurrency controller
    def execute_query_wrapper(query_text: str, group: QueryGroup):
        query_name = f"{group.value}_query"
        monitor.start_request(query_name, group)
        success = load_tester._execute_query(query_text, query_name)
        monitor.end_request(query_name)
        return success
    
    controller = ConcurrencyController(execute_query_wrapper)
    
    # Set initial concurrency per group
    concurrency_config = {
        QueryGroup.TERMS_AGGREGATION: 3,
        QueryGroup.SORTING: 2,
        QueryGroup.TEXT_QUERYING: 2,
        QueryGroup.RANGE_QUERIES: 2,
        QueryGroup.MATH_FUNCTIONS: 1
    }
    
    print("Starting constant concurrency test...")
    controller.start()
    
    # Set concurrency for each group
    for group, concurrency in concurrency_config.items():
        if group in query_groups:
            controller.set_concurrency(group, concurrency, query_groups[group])
            print(f"Set {group.value}: {concurrency} concurrent queries")
    
    start_time = time.time()
    
    try:
        # Run for specified duration with concurrency adjustments
        while time.time() - start_time < args.duration:
            time.sleep(10)  # Check every 10 seconds
            
            # Show current status
            total_active = 0
            status_parts = []
            for group in query_groups:
                active = controller.get_active_count(group)
                total_active += active
                status_parts.append(f"{group.value}: {active}")
            
            elapsed = int(time.time() - start_time)
            print(f"[{elapsed}s] Active workers - {' | '.join(status_parts)} | Total: {total_active}")
            
            # Example: Ramp up after 60 seconds
            if elapsed == 60:
                print("Ramping up concurrency...")
                for group in concurrency_config:
                    if group in query_groups:
                        new_concurrency = concurrency_config[group] * 2
                        controller.update_concurrency(group, new_concurrency)
    
    except KeyboardInterrupt:
        print("\\nStopping test...")
    
    finally:
        controller.stop()
        print("Test completed")
        
        # Show final stats
        print("\\nFinal Statistics:")
        for group in query_groups:
            group_stats = monitor.get_group_stats(group)
            if group_stats:
                print(f"{group.value}: {group_stats['total_requests']} requests, "
                      f"success rate {group_stats['success_rate']:.1f}%")

if __name__ == '__main__':
    main()
#!/usr/bin/env python3
import argparse
import time
import json
import glob
from opensearchpy import OpenSearch
from src.loadtest.config import LoadTestConfig

def run_simple_benchmark():
    parser = argparse.ArgumentParser(description='Simple benchmark - run queries sequentially')
    parser.add_argument('--host', default='localhost', help='OpenSearch host')
    parser.add_argument('--port', default=9200, type=int, help='OpenSearch port')
    parser.add_argument('--username', help='OpenSearch username')
    parser.add_argument('--password', help='OpenSearch password')
    parser.add_argument('--ssl', action='store_true', help='Use SSL')
    parser.add_argument('--index', default='*', help='Index pattern')
    parser.add_argument('--warm-iterations', default=10, type=int, help='Warm-up iterations')
    parser.add_argument('--iterations', default=10, type=int, help='Actual benchmark iterations')
    
    args = parser.parse_args()
    
    # Create OpenSearch client
    client_config = {
        'hosts': [{'host': args.host, 'port': args.port}],
        'use_ssl': args.ssl,
        'verify_certs': False
    }
    if args.username and args.password:
        client_config['http_auth'] = (args.username, args.password)
    
    client = OpenSearch(**client_config)
    
    # Load all queries
    query_files = glob.glob('queries/*.ppl')
    if not query_files:
        print("No .ppl files found in queries/ directory")
        return
    
    print(f"Found {len(query_files)} queries")
    print(f"Warm-up iterations: {args.warm_iterations}")
    print(f"Benchmark iterations: {args.iterations}")
    print()
    
    results = {}
    
    for query_file in sorted(query_files):
        query_name = query_file.split('/')[-1].replace('.ppl', '')
        
        with open(query_file, 'r') as f:
            query = f.read().strip()
            # Replace all source patterns
            query = query.replace('{index}', args.index)
            # Replace common source patterns in PPL
            import re
            query = re.sub(r'source\s*=\s*[^\s|]+', f'source = {args.index}', query)
            query = re.sub(r'search\s+source\s*=\s*[^\s|]+', f'search source = {args.index}', query)
        
        print(f"Running {query_name}...")
        
        # Warm-up
        for i in range(args.warm_iterations):
            try:
                client.transport.perform_request('POST', '/_plugins/_ppl', body={"query": query}, timeout=300)
            except Exception as e:
                print(f"  Warm-up {i+1} failed: {e}")
        
        # Benchmark
        durations = []
        for i in range(args.iterations):
            start_time = time.time()
            try:
                client.transport.perform_request('POST', '/_plugins/_ppl', body={"query": query}, timeout=300)
                duration = (time.time() - start_time) * 1000  # Convert to ms
                durations.append(duration)
            except Exception as e:
                print(f"  Iteration {i+1} failed: {e}")
        
        if durations:
            durations.sort()
            p90_index = int(0.9 * len(durations))
            p90 = durations[p90_index] if p90_index < len(durations) else durations[-1]
            
            results[query_name] = {
                'p90_ms': round(p90, 2),
                'min_ms': round(min(durations), 2),
                'max_ms': round(max(durations), 2),
                'avg_ms': round(sum(durations) / len(durations), 2),
                'successful_runs': len(durations)
            }
            
            print(f"  P90: {results[query_name]['p90_ms']}ms")
        else:
            print(f"  All iterations failed")
    
    # Print summary
    print("\n=== Benchmark Results ===")
    print(f"{'Query':<30} {'P90 (ms)':<10} {'Min (ms)':<10} {'Max (ms)':<10} {'Avg (ms)':<10}")
    print("-" * 70)
    
    for query_name, metrics in sorted(results.items()):
        print(f"{query_name:<30} {metrics['p90_ms']:<10} {metrics['min_ms']:<10} {metrics['max_ms']:<10} {metrics['avg_ms']:<10}")

if __name__ == '__main__':
    run_simple_benchmark()
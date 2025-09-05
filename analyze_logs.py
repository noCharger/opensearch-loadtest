#!/usr/bin/env python3
import json
import glob
import sys
from collections import defaultdict
from opensearchpy import OpenSearch
import time
import argparse

def load_logs(execution_id):
    """Load query metrics from QUERY_METRICS.log file (actual test only)"""
    query_metrics_file = f"logs/{execution_id}_QUERY_METRICS.log"
    
    query_metrics = []
    query_metrics_raw = []  # Keep raw data for concurrency lookup
    
    try:
        total_lines = 0
        warmup_skipped = 0
        phase_skipped = 0
        success_found = 0
        
        with open(query_metrics_file, 'r') as f:
            for line in f:
                total_lines += 1
                try:
                    entry = json.loads(line.strip())
                    
                    # Only process actual test queries (warmup is in separate file)
                    query_name = entry.get('query_name', '')
                    timestamp = entry.get('@timestamp', 0) / 1000  # Convert from milliseconds
                    

                    # Only include queries with latency data
                    if entry.get('query_latency'):
                        success_found += 1
                        query_metrics.append({
                            'query_name': query_name,
                            'timestamp': timestamp,
                            'duration': entry['query_latency']  # Already in milliseconds
                        })
                        query_metrics_raw.append(entry)  # Keep raw entry for concurrency data
                except json.JSONDecodeError:
                    continue
        
        print(f"Debug: Total lines: {total_lines}, Success found: {success_found} (warmup data in separate file)")
    except FileNotFoundError:
        print(f"Warning: {query_metrics_file} not found, falling back to individual log files")
        # Fallback to old method if QUERY_METRICS.log doesn't exist
        return load_logs_fallback(execution_id), []
    
    return query_metrics, query_metrics_raw

def load_benchmark_metrics(execution_id):
    """Load benchmark metrics from METRICS.log file (actual test only)"""
    metrics_file = f"logs/{execution_id}_METRICS.log"
    benchmark_metrics = []
    
    try:
        with open(metrics_file, 'r') as f:
            for line in f:
                try:
                    entry = json.loads(line.strip())
                    # Only include metrics with non-zero max_concurrency (active test phase)
                    if entry.get('max_concurrency', 0) > 0:
                        benchmark_metrics.append(entry)
                except json.JSONDecodeError:
                    continue
    except FileNotFoundError:
        print(f"Warning: {metrics_file} not found")
    
    return benchmark_metrics

def analyze_data_node_cpu(benchmark_metrics):
    """Analyze CPU usage for data nodes"""
    data_node_cpu = {}
    
    for metric in benchmark_metrics:
        if metric.get('node_type') == 'data' and 'os_cpu_percent' in metric:
            node_name = metric.get('node_name', metric.get('node_id', 'unknown'))
            cpu_percent = metric.get('os_cpu_percent')
            timestamp = metric.get('@timestamp', 0) // 1000  # Convert to seconds
            
            if node_name not in data_node_cpu:
                data_node_cpu[node_name] = []
            data_node_cpu[node_name].append({'timestamp': timestamp, 'cpu': cpu_percent})
    
    # Calculate stats for each node
    node_stats = {}
    for node_name, cpu_data in data_node_cpu.items():
        if cpu_data:
            cpu_values = [d['cpu'] for d in cpu_data]
            node_stats[node_name] = {
                'avg_cpu': sum(cpu_values) / len(cpu_values),
                'max_cpu': max(cpu_values),
                'min_cpu': min(cpu_values),
                'samples': len(cpu_values)
            }
    
    return node_stats

def analyze_data_node_jvm_memory(benchmark_metrics):
    """Analyze JVM memory usage for data nodes"""
    data_node_jvm = {}
    
    for metric in benchmark_metrics:
        if metric.get('node_type') == 'data':
            node_name = metric.get('node_name', metric.get('node_id', 'unknown'))
            timestamp = metric.get('@timestamp', 0) // 1000
            
            jvm_data = {}
            if 'jvm_mem_heap_used_percent' in metric:
                jvm_data['heap_percent'] = metric['jvm_mem_heap_used_percent']
            if 'jvm_mem_heap_used_in_bytes' in metric:
                jvm_data['heap_used_bytes'] = metric['jvm_mem_heap_used_in_bytes']
            if 'jvm_mem_heap_max_in_bytes' in metric:
                jvm_data['heap_max_bytes'] = metric['jvm_mem_heap_max_in_bytes']
            
            if jvm_data:
                if node_name not in data_node_jvm:
                    data_node_jvm[node_name] = []
                jvm_data['timestamp'] = timestamp
                data_node_jvm[node_name].append(jvm_data)
    
    # Calculate JVM memory stats
    node_jvm_stats = {}
    for node_name, jvm_data_list in data_node_jvm.items():
        if jvm_data_list:
            heap_values = [d.get('heap_percent', 0) for d in jvm_data_list if d.get('heap_percent')]
            heap_used_values = [d.get('heap_used_bytes', 0) for d in jvm_data_list if d.get('heap_used_bytes')]
            heap_max_values = [d.get('heap_max_bytes', 0) for d in jvm_data_list if d.get('heap_max_bytes')]
            
            if heap_values:
                node_jvm_stats[node_name] = {
                    'avg_heap_percent': sum(heap_values) / len(heap_values),
                    'max_heap_percent': max(heap_values),
                    'min_heap_percent': min(heap_values),
                    'avg_heap_used_gb': (sum(heap_used_values) / len(heap_used_values)) / (1024**3) if heap_used_values else 0,
                    'max_heap_gb': max(heap_max_values) / (1024**3) if heap_max_values else 0,
                    'samples': len(jvm_data_list)
                }
    
    return node_jvm_stats

def analyze_query_latency_by_type(query_metrics):
    """Analyze query latency by query type (PPL vs DSL)"""
    query_type_stats = defaultdict(list)
    
    for metric in query_metrics:
        query_name = metric['query_name']
        latency_ms = metric['duration']  # Already in milliseconds
        
        # Determine query type based on name patterns
        if any(dsl_pattern in query_name.lower() for dsl_pattern in ['match_all', 'term_search', 'range_', 'dsl_', 'bool_']):
            query_type_stats['DSL'].append(latency_ms)
        else:
            query_type_stats['PPL'].append(latency_ms)
    
    # Calculate statistics for each type
    type_summary = {}
    for query_type, latencies in query_type_stats.items():
        if latencies:
            sorted_latencies = sorted(latencies)
            count = len(latencies)
            type_summary[query_type] = {
                'count': count,
                'avg_latency_ms': sum(latencies) / count,
                'min_latency_ms': min(latencies),
                'max_latency_ms': max(latencies),
                'p50_latency_ms': sorted_latencies[int(0.5 * count)],
                'p90_latency_ms': sorted_latencies[int(0.9 * count)],
                'p99_latency_ms': sorted_latencies[int(0.99 * count)] if count > 100 else sorted_latencies[-1]
            }
    
    return type_summary

def load_logs_fallback(execution_id):
    """Fallback method to load from individual query log files"""
    log_pattern = f"logs/{execution_id}_*.log"
    log_files = glob.glob(log_pattern)
    
    query_metrics = []
    for log_file in log_files:
        query_name = log_file.split('_', 1)[1].replace('.log', '')
        if query_name in ['EXECUTION', 'METRICS', 'TIMELINE', 'QUERY_METRICS']:
            continue
            
        with open(log_file, 'r') as f:
            for line in f:
                try:
                    entry = json.loads(line.strip())
                    # Skip warmup events
                    if entry.get('event_type') in ['WARMUP_SUCCESS', 'WARMUP_ERROR']:
                        continue
                    # Skip events during warmup phase
                    if warmup_start and warmup_end and warmup_start <= entry.get('timestamp', 0) <= warmup_end:
                        continue
                    # Skip warmup query names
                    if query_name.startswith('warmup_'):
                        continue
                        
                    if entry.get('event_type') == 'SUCCESS' and entry.get('duration'):
                        query_metrics.append({
                            'query_name': query_name,
                            'timestamp': entry['timestamp'],
                            'duration': entry['duration']  # Already in milliseconds
                        })
                except json.JSONDecodeError:
                    continue
    
    return query_metrics

def get_warmup_boundaries(execution_id):
    """Get warmup phase start and end timestamps from execution log"""
    execution_file = f"logs/{execution_id}_EXECUTION.log"
    warmup_start = None
    warmup_end = None
    
    try:
        with open(execution_file, 'r') as f:
            for line in f:
                try:
                    entry = json.loads(line.strip())
                    if entry.get('event_type') == 'WARMUP_START':
                        warmup_start = entry.get('timestamp')
                    elif entry.get('event_type') == 'WARMUP_END':
                        warmup_end = entry.get('timestamp')
                except json.JSONDecodeError:
                    continue
    except FileNotFoundError:
        pass
    
    return warmup_start, warmup_end

def load_execution_events(execution_id):
    """Load execution events from EXECUTION.log file, excluding warmup events"""
    execution_file = f"logs/{execution_id}_EXECUTION.log"
    execution_events = []
    
    try:
        with open(execution_file, 'r') as f:
            for line in f:
                try:
                    entry = json.loads(line.strip())
                    # Exclude warmup-related events from main analysis
                    if entry.get('event_type') not in ['WARMUP_START', 'WARMUP_END']:
                        execution_events.append(entry)
                except json.JSONDecodeError:
                    continue
    except FileNotFoundError:
        print(f"Warning: {execution_file} not found")
    
    return execution_events

def calculate_concurrency_per_query(query_metrics):
    """Get max concurrency per query from actual metrics (not calculated)"""
    if not query_metrics:
        return {}
    
    # Just return the query names with placeholder values
    # Real max_concurrency comes from the monitoring system
    query_names = set(metric['query_name'] for metric in query_metrics)
    return {name: 0 for name in query_names}  # Placeholder values

def calculate_p90_per_second(query_metrics, query_metrics_raw):
    """Calculate P90 latency per second per query with actual concurrency for that second"""
    if not query_metrics:
        return []
    
    # Group raw data by second for concurrency lookup
    second_concurrency = defaultdict(lambda: defaultdict(list))
    for entry in query_metrics_raw:
        timestamp = entry.get('@timestamp', 0) / 1000
        second = int(timestamp)
        query_name = entry.get('query_name', '')
        if query_name:
            second_concurrency[second][query_name].append({
                'query_max_concurrency': entry.get('query_max_concurrency', 0),
                'total_max_concurrency': entry.get('total_max_concurrency', 0)
            })
    
    # Group by second and query name for P90
    second_query_buckets = defaultdict(lambda: defaultdict(list))
    for metric in query_metrics:
        second = int(metric['timestamp'])
        second_query_buckets[second][metric['query_name']].append(metric['duration'])
    
    # Generate docs with P90 and actual concurrency for that second
    p90_data = []
    for second, query_buckets in second_query_buckets.items():
        for query_name, durations in query_buckets.items():
            # Get actual concurrency values for this second
            concurrency_entries = second_concurrency.get(second, {}).get(query_name, [])
            if concurrency_entries:
                max_conc = max(c['query_max_concurrency'] for c in concurrency_entries)
                total_max_conc = max(c['total_max_concurrency'] for c in concurrency_entries)
            else:
                max_conc = 0
                total_max_conc = 0
            
            sorted_durations = sorted(durations)
            p90_index = int(0.9 * len(sorted_durations))
            p90 = sorted_durations[p90_index] if p90_index < len(sorted_durations) else sorted_durations[-1]
            
            p90_data.append({
                'timestamp': second,
                'query_name': query_name,
                'p90_latency': p90,
                'query_count': len(durations),
                'query_max_concurrency': max_conc,
                'total_max_concurrency': total_max_conc
            })
    
    return p90_data

def export_to_opensearch(p90_data, execution_id, client):
    """Export P90 metrics with max concurrency to OpenSearch using bulk API"""
    index_name = f"query-p90-metrics-{time.strftime('%Y-%m')}-{execution_id}"
    
    # Create index with mapping if it doesn't exist
    if not client.indices.exists(index=index_name):
        mapping = {
            "mappings": {
                "properties": {
                    "@timestamp": {"type": "date", "format": "epoch_millis"},
                    "test-execution-id": {"type": "keyword"},
                    "query_name": {"type": "keyword"},
                    "p90_latency_ms": {"type": "float"},
                    "query_count": {"type": "integer"},
                    "query_max_concurrency": {"type": "integer"},
                    "total_max_concurrency": {"type": "integer"}
                }
            }
        }
        client.indices.create(index=index_name, body=mapping)
        print(f"Created index: {index_name}")
    
    # Prepare bulk data
    bulk_data = []
    for data in p90_data:
        bulk_data.append({"index": {"_index": index_name}})
        bulk_data.append({
            '@timestamp': data['timestamp'] * 1000,
            'test-execution-id': execution_id,
            'query_name': data['query_name'],
            'p90_latency_ms': data['p90_latency'],  # Already in milliseconds
            'query_count': data['query_count'],
            'query_max_concurrency': data['query_max_concurrency'],
            'total_max_concurrency': data['total_max_concurrency']
        })
    
    # Bulk upload in chunks
    chunk_size = 1000
    for i in range(0, len(bulk_data), chunk_size * 2):  # *2 because each doc has 2 lines
        chunk = bulk_data[i:i + chunk_size * 2]
        try:
            response = client.bulk(body=chunk)
            if response.get('errors'):
                print(f"Bulk upload errors: {response['errors']}")
        except Exception as e:
            print(f"Failed bulk upload: {e}")

def export_benchmark_metrics(benchmark_metrics, execution_id, client):
    """Export benchmark metrics to OpenSearch using bulk API"""
    if not benchmark_metrics:
        return
        
    index_name = f"benchmark-metrics-{time.strftime('%Y-%m')}-{execution_id}"
    
    # Create index with mapping if it doesn't exist
    if not client.indices.exists(index=index_name):
        mapping = {
            "mappings": {
                "dynamic_templates": [
                    {
                        "strings": {
                            "match": "*",
                            "match_mapping_type": "string",
                            "mapping": {"type": "keyword"}
                        }
                    }
                ],
                "date_detection": False,
                "properties": {
                    "@timestamp": {"type": "date", "format": "epoch_millis"},
                    "test-execution-id": {"type": "keyword"},
                    "environment": {"type": "keyword"},
                    "job": {"type": "keyword"}
                }
            },
            "settings": {
                "number_of_shards": 3,
                "number_of_replicas": 2,
                "mapping.total_fields.limit": 2000
            }
        }
        client.indices.create(index=index_name, body=mapping)
        print(f"Created benchmark metrics index: {index_name}")
    
    # Prepare bulk data
    bulk_data = []
    for metric in benchmark_metrics:
        bulk_data.append({"index": {"_index": index_name}})
        bulk_data.append(metric)
    
    # Bulk upload in chunks
    chunk_size = 1000
    for i in range(0, len(bulk_data), chunk_size * 2):
        chunk = bulk_data[i:i + chunk_size * 2]
        try:
            response = client.bulk(body=chunk)
            if response.get('errors'):
                print(f"Benchmark metrics bulk upload errors: {response['errors']}")
        except Exception as e:
            print(f"Failed benchmark metrics bulk upload: {e}")

def export_execution_events(execution_events, execution_id, client):
    """Export execution events to OpenSearch using bulk API"""
    if not execution_events:
        return
        
    index_name = f"execution-timeline-{time.strftime('%Y-%m')}-{execution_id}"
    
    # Create index with mapping if it doesn't exist
    if not client.indices.exists(index=index_name):
        mapping = {
            "mappings": {
                "properties": {
                    "@timestamp": {"type": "date", "format": "epoch_millis"},
                    "test-execution-id": {"type": "keyword"},
                    "event_type": {"type": "keyword"},
                    "query_name": {"type": "keyword"}
                }
            }
        }
        client.indices.create(index=index_name, body=mapping)
        print(f"Created execution timeline index: {index_name}")
    
    # Prepare bulk data
    bulk_data = []
    for event in execution_events:
        bulk_data.append({"index": {"_index": index_name}})
        bulk_data.append({
            '@timestamp': event['timestamp'] * 1000,
            'test-execution-id': execution_id,
            'event_type': event['event_type'],
            'query_name': event['query_name']
        })
    
    # Bulk upload in chunks
    chunk_size = 1000
    for i in range(0, len(bulk_data), chunk_size * 2):
        chunk = bulk_data[i:i + chunk_size * 2]
        try:
            response = client.bulk(body=chunk)
            if response.get('errors'):
                print(f"Execution events bulk upload errors: {response['errors']}")
        except Exception as e:
            print(f"Failed execution events bulk upload: {e}")

def main():
    parser = argparse.ArgumentParser(description='Analyze load test logs')
    parser.add_argument('execution_id', help='Execution ID to analyze')
    parser.add_argument('--host', default='localhost', help='OpenSearch host')
    parser.add_argument('--port', default=9200, type=int, help='OpenSearch port')
    parser.add_argument('--username', help='OpenSearch username')
    parser.add_argument('--password', help='OpenSearch password')
    parser.add_argument('--ssl', action='store_true', help='Use SSL')
    
    args = parser.parse_args()
    
    # Check for warmup phase
    warmup_start, warmup_end = get_warmup_boundaries(args.execution_id)
    if warmup_start and warmup_end:
        warmup_duration = warmup_end - warmup_start
        print(f"Detected warmup phase: {warmup_duration:.1f} seconds (excluded from analysis)")
    
    # Load logs
    print(f"Loading logs for execution_id: {args.execution_id}")
    query_metrics, query_metrics_raw = load_logs(args.execution_id)
    print(f"Loaded {len(query_metrics)} query metrics (warmup excluded)")
    
    # Load benchmark metrics (warmup data included for system monitoring)
    benchmark_metrics = load_benchmark_metrics(args.execution_id)
    print(f"Loaded {len(benchmark_metrics)} benchmark metrics")
    
    # Load execution events
    execution_events = load_execution_events(args.execution_id)
    print(f"Loaded {len(execution_events)} execution events (warmup excluded)")
    
    # Calculate max concurrency per query type (main test phase only)
    max_concurrency = calculate_concurrency_per_query(query_metrics)
    print(f"\nMax Concurrency per Query Type (main test phase only):")
    print("Query Name\t\t\t\tMax Concurrency")
    print("-" * 50)
    for query_name, max_conc in sorted(max_concurrency.items()):
        query_display = query_name[:30] + '...' if len(query_name) > 30 else query_name
        print(f"{query_display:<32}\t{max_conc}")
    
    # Calculate P90 per second (main test phase only)
    p90_data = calculate_p90_per_second(query_metrics, query_metrics_raw)
    print(f"\nGenerated {len(p90_data)} P90 data points (main test phase only)")
    print("Sample entries:")
    print("Timestamp\t\tQuery Name\t\t\tP90 Latency\tCount")
    for data in p90_data[:10]:  # Show first 10 entries
        ts = time.strftime('%H:%M:%S', time.localtime(data['timestamp']))
        query_name = data['query_name'][:20] + '...' if len(data['query_name']) > 20 else data['query_name']
        print(f"{ts}\t\t{query_name:<24}\t{data['p90_latency']:.2f}ms\t\t{data['query_count']}")
    
    # Analyze data node CPU usage (includes warmup for system monitoring)
    cpu_stats = analyze_data_node_cpu(benchmark_metrics)
    if cpu_stats:
        print(f"\nData Node CPU Usage (entire test including warmup):")
        print("Node Name\t\t\tAvg CPU\tMax CPU\tMin CPU\tSamples")
        print("-" * 65)
        for node_name, stats in sorted(cpu_stats.items()):
            node_display = node_name[:20] + '...' if len(node_name) > 20 else node_name
            print(f"{node_display:<24}\t{stats['avg_cpu']:.1f}%\t{stats['max_cpu']:.1f}%\t{stats['min_cpu']:.1f}%\t{stats['samples']}")
    else:
        print("\nNo data node CPU metrics found (requires updated metrics collection)")
    
    # Analyze data node JVM memory (includes warmup for system monitoring)
    jvm_stats = analyze_data_node_jvm_memory(benchmark_metrics)
    if jvm_stats:
        print(f"\nData Node JVM Memory (entire test including warmup):")
        print("Node Name\t\t\tAvg Heap%\tMax Heap%\tMin Heap%\tAvg Used GB\tMax Heap GB\tSamples")
        print("-" * 90)
        for node_name, stats in sorted(jvm_stats.items()):
            node_display = node_name[:20] + '...' if len(node_name) > 20 else node_name
            print(f"{node_display:<24}\t{stats['avg_heap_percent']:.1f}%\t\t{stats['max_heap_percent']:.1f}%\t\t{stats['min_heap_percent']:.1f}%\t\t{stats['avg_heap_used_gb']:.2f}\t\t{stats['max_heap_gb']:.2f}\t\t{stats['samples']}")
    else:
        print("\nNo data node JVM memory metrics found")
    
    # Analyze query latency by type (main test phase only)
    latency_by_type = analyze_query_latency_by_type(query_metrics)
    if latency_by_type:
        print(f"\nQuery Latency by Type (main test phase only):")
        print("Type\tCount\tAvg (ms)\tMin (ms)\tMax (ms)\tP50 (ms)\tP90 (ms)\tP99 (ms)")
        print("-" * 75)
        for query_type, stats in sorted(latency_by_type.items()):
            print(f"{query_type}\t{stats['count']}\t{stats['avg_latency_ms']:.1f}\t\t{stats['min_latency_ms']:.1f}\t\t{stats['max_latency_ms']:.1f}\t\t{stats['p50_latency_ms']:.1f}\t\t{stats['p90_latency_ms']:.1f}\t\t{stats['p99_latency_ms']:.1f}")
    else:
        print("\nNo query latency data found")
    
    # Export to OpenSearch
    if args.username and args.password:
        auth = (args.username, args.password)
        client = OpenSearch(
            hosts=[{'host': args.host, 'port': args.port}],
            http_auth=auth,
            use_ssl=args.ssl,
            verify_certs=False
        )
        
        print(f"\nExporting {len(p90_data)} P90 documents to OpenSearch (main test phase only)...")
        export_to_opensearch(p90_data, args.execution_id, client)
        print("P90 metrics export completed (warmup excluded)")
        
        print(f"\nExporting {len(benchmark_metrics)} benchmark metrics to OpenSearch...")
        export_benchmark_metrics(benchmark_metrics, args.execution_id, client)
        print("Benchmark metrics export completed")
        
        print(f"\nExporting {len(execution_events)} execution events to OpenSearch (warmup excluded)...")
        export_execution_events(execution_events, args.execution_id, client)
        print("Execution events export completed (warmup excluded)")
        
        # Export data node metrics summary if available
        if cpu_stats:
            print(f"\nData node CPU summary exported with benchmark metrics (entire test including warmup)")
        
        jvm_stats = analyze_data_node_jvm_memory(benchmark_metrics)
        if jvm_stats:
            print(f"\nData node JVM memory summary exported with benchmark metrics (entire test including warmup)")
        
        latency_by_type = analyze_query_latency_by_type(query_metrics)
        if latency_by_type:
            print(f"\nQuery latency by type analysis exported with benchmark metrics (warmup excluded)")

if __name__ == '__main__':
    main()
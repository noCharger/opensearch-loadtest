import time
import json
import os
from typing import Dict, Any
from opensearchpy import OpenSearch

class MetricsExporter:
    def __init__(self, source_client: OpenSearch, metrics_client: OpenSearch, execution_id: str, observability_monitor=None):
        self.source_client = source_client  # Client to get stats from
        self.metrics_client = metrics_client  # Client to export metrics to
        self.execution_id = execution_id
        self.observability_monitor = observability_monitor
        self.metrics_index = f"benchmark-metrics-{time.strftime('%Y-%m')}-{execution_id}"
        self.query_metrics_index = f"query-metrics-{time.strftime('%Y-%m')}-{execution_id}"
        self.metrics_log_file = f"logs/{execution_id}_METRICS.log"
        self.warmup_metrics_log_file = f"logs/{execution_id}_WARMUP_METRICS.log"
        self.is_warmup_phase = False
        self.pending_metrics = []  # Buffer for bulk upload
        self.last_bulk_upload = time.time()
        os.makedirs("logs", exist_ok=True)
        self._ensure_index()
        self._ensure_query_metrics_index()
    
    def _ensure_index(self):
        """Create metrics index with proper mapping if it doesn't exist"""
        if not self.metrics_client.indices.exists(index=self.metrics_index):
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
            self.metrics_client.indices.create(index=self.metrics_index, body=mapping)
    
    def _ensure_query_metrics_index(self):
        """Create query metrics index with proper mapping"""
        if not self.metrics_client.indices.exists(index=self.query_metrics_index):
            mapping = {
                "mappings": {
                    "properties": {
                        "@timestamp": {"type": "date", "format": "epoch_millis"},
                        "test-execution-id": {"type": "keyword"},
                        "query_name": {"type": "keyword"},
                        "query_latency": {"type": "float"},
                        "max_concurrency": {"type": "integer"}
                    }
                },
                "settings": {
                    "number_of_shards": 3,
                    "number_of_replicas": 2
                }
            }
            self.metrics_client.indices.create(index=self.query_metrics_index, body=mapping)
    
    def set_warmup_phase(self, is_warmup: bool):
        """Set whether we're in warmup phase"""
        self.is_warmup_phase = is_warmup
        print(f"MetricsExporter: Warmup phase set to {is_warmup}")
    
    def export_node_stats(self, environment: str = "loadtest") -> bool:
        """Export node stats to metrics index - data nodes only with CPU and JVM memory"""
        try:
            nodes_info = self.source_client.nodes.info()
            stats = self.source_client.nodes.stats()
            timestamp = int(time.time() * 1000)
            
            for node_id, node_data in stats['nodes'].items():
                node_info = nodes_info.get('nodes', {}).get(node_id, {})
                roles = node_info.get('roles', [])
                
                # Only export metrics for data nodes
                if 'data' not in roles:
                    continue
                
                doc = self._extract_data_node_metrics(node_data)
                # Get current max concurrency from observability monitor
                current_max_concurrency = self.observability_monitor.get_max_concurrency() if self.observability_monitor else 0
                
                doc.update({
                    "@timestamp": timestamp,
                    "test-execution-id": self.execution_id,
                    "environment": environment,
                    "job": "opensearch-loadtest",
                    "max_concurrency": current_max_concurrency,
                    "node_id": node_id,
                    "node_name": node_info.get('name', node_id[:8]),
                    "node_ip": node_info.get('ip', ''),
                    "node_type": "data"
                })
                
                # Log to appropriate file based on phase
                self._log_to_file(doc, is_warmup=self.is_warmup_phase)
                
                # Add to bulk buffer
                self.pending_metrics.append({"index": {"_index": self.metrics_index}})
                self.pending_metrics.append(doc)
                
                # Check if we should bulk upload (every 10 seconds for frequent collection)
                if time.time() - self.last_bulk_upload >= 10:
                    self._bulk_upload_metrics()
            return True
        except Exception as e:
            print(f"Failed to export node stats: {e}")
            return False
    
    def _get_primary_node_type(self, roles: list) -> str:
        """Determine primary node type from roles"""
        if 'data' in roles:
            return 'data'
        elif 'master' in roles:
            return 'master'
        elif 'ingest' in roles:
            return 'ingest'
        else:
            return 'coordinating'
    
    def _extract_data_node_metrics(self, node_data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract comprehensive metrics for data nodes"""
        metrics = {}
        
        # CPU metrics
        if 'os' in node_data and 'cpu' in node_data['os']:
            cpu = node_data['os']['cpu']
            metrics['os_cpu_percent'] = cpu.get('percent')
            if 'load_average' in cpu:
                load_avg = cpu['load_average']
                metrics.update({
                    'os_cpu_load_average_1m': load_avg.get('1m'),
                    'os_cpu_load_average_5m': load_avg.get('5m'),
                    'os_cpu_load_average_15m': load_avg.get('15m')
                })
        
        # Memory metrics
        if 'os' in node_data and 'mem' in node_data['os']:
            mem = node_data['os']['mem']
            metrics.update({
                'os_mem_total_in_bytes': mem.get('total_in_bytes'),
                'os_mem_free_in_bytes': mem.get('free_in_bytes'),
                'os_mem_used_in_bytes': mem.get('used_in_bytes'),
                'os_mem_free_percent': mem.get('free_percent'),
                'os_mem_used_percent': mem.get('used_percent')
            })
        
        # Complete JVM metrics
        if 'jvm' in node_data:
            jvm = node_data['jvm']
            
            # JVM Memory
            if 'mem' in jvm:
                jvm_mem = jvm['mem']
                metrics.update({
                    'jvm_mem_heap_used_in_bytes': jvm_mem.get('heap_used_in_bytes'),
                    'jvm_mem_heap_max_in_bytes': jvm_mem.get('heap_max_in_bytes'),
                    'jvm_mem_heap_used_percent': jvm_mem.get('heap_used_percent'),
                    'jvm_mem_heap_committed_in_bytes': jvm_mem.get('heap_committed_in_bytes'),
                    'jvm_mem_non_heap_used_in_bytes': jvm_mem.get('non_heap_used_in_bytes'),
                    'jvm_mem_non_heap_committed_in_bytes': jvm_mem.get('non_heap_committed_in_bytes')
                })
                
                # JVM Memory Pools
                if 'pools' in jvm_mem:
                    pools = jvm_mem['pools']
                    for pool_name, pool_data in pools.items():
                        pool_prefix = f'jvm_mem_pool_{pool_name.replace(" ", "_").lower()}'
                        metrics.update({
                            f'{pool_prefix}_used_in_bytes': pool_data.get('used_in_bytes'),
                            f'{pool_prefix}_max_in_bytes': pool_data.get('max_in_bytes'),
                            f'{pool_prefix}_peak_used_in_bytes': pool_data.get('peak_used_in_bytes'),
                            f'{pool_prefix}_peak_max_in_bytes': pool_data.get('peak_max_in_bytes')
                        })
            
            # JVM GC
            if 'gc' in jvm:
                gc = jvm['gc']
                if 'collectors' in gc:
                    for gc_name, gc_data in gc['collectors'].items():
                        gc_prefix = f'jvm_gc_{gc_name.replace(" ", "_").lower()}'
                        metrics.update({
                            f'{gc_prefix}_collection_count': gc_data.get('collection_count'),
                            f'{gc_prefix}_collection_time_in_millis': gc_data.get('collection_time_in_millis')
                        })
            
            # JVM Threads
            if 'threads' in jvm:
                threads = jvm['threads']
                metrics.update({
                    'jvm_threads_count': threads.get('count'),
                    'jvm_threads_peak_count': threads.get('peak_count')
                })
            
            # JVM Buffer Pools
            if 'buffer_pools' in jvm:
                for pool_name, pool_data in jvm['buffer_pools'].items():
                    pool_prefix = f'jvm_buffer_pool_{pool_name.replace(" ", "_").lower()}'
                    metrics.update({
                        f'{pool_prefix}_count': pool_data.get('count'),
                        f'{pool_prefix}_used_in_bytes': pool_data.get('used_in_bytes'),
                        f'{pool_prefix}_total_capacity_in_bytes': pool_data.get('total_capacity_in_bytes')
                    })
        
        return {k: v for k, v in metrics.items() if v is not None}
    
    def _log_to_file(self, doc: Dict[str, Any], is_warmup: bool = False):
        """Log metrics document to appropriate file"""
        try:
            log_file = self.warmup_metrics_log_file if is_warmup else self.metrics_log_file
            with open(log_file, 'a') as f:
                f.write(json.dumps(doc) + '\n')
            if is_warmup:
                print(f"Logged warmup metrics to {log_file}")
        except Exception as e:
            print(f"Failed to log metrics to file: {e}")
    
    def _bulk_upload_metrics(self):
        """Bulk upload pending metrics to OpenSearch"""
        if not self.pending_metrics:
            return
        
        try:
            response = self.metrics_client.bulk(body=self.pending_metrics)
            if response.get('errors'):
                print(f"Bulk metrics upload errors: {response['errors']}")
            else:
                print(f"Bulk uploaded {len(self.pending_metrics)//2} metrics documents")
            
            self.pending_metrics.clear()
            self.last_bulk_upload = time.time()
        except Exception as e:
            print(f"Failed bulk metrics upload: {e}")
    
    def flush_pending_metrics(self):
        """Force upload any pending metrics (called at test end)"""
        if self.pending_metrics:
            print("Flushing pending metrics...")
            self._bulk_upload_metrics()
    
    def export_query_metrics(self, query_name: str, query_latency: float, is_warmup: bool = False) -> bool:
        """Export query metrics to query metrics index - latency in milliseconds"""
        try:
            timestamp = int(time.time() * 1000)
            doc = {
                "@timestamp": timestamp,
                "test-execution-id": self.execution_id,
                "query_name": query_name,
                "query_latency": query_latency,  # Already in milliseconds
                "query_max_concurrency": self.observability_monitor.get_query_max_concurrency(query_name) if self.observability_monitor else 0,
                "total_max_concurrency": self.observability_monitor.get_max_concurrency() if self.observability_monitor else 0
            }
            
            # Log to separate files for warmup vs actual test
            if is_warmup:
                query_log_file = f"logs/{self.execution_id}_WARMUP_QUERY_METRICS.log"
            else:
                query_log_file = f"logs/{self.execution_id}_QUERY_METRICS.log"
            
            try:
                with open(query_log_file, 'a') as f:
                    f.write(json.dumps(doc) + '\n')
            except Exception as e:
                print(f"Failed to log query metrics to file: {e}")
            
            # Add to bulk buffer
            self.pending_metrics.append({"index": {"_index": self.query_metrics_index}})
            self.pending_metrics.append(doc)
            
            # Check if we should bulk upload
            if time.time() - self.last_bulk_upload >= 60:
                self._bulk_upload_metrics()
            
            return True
        except Exception as e:
            print(f"Failed to export query metrics: {e}")
            return False
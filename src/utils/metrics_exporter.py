import time
import json
from typing import Dict, Any
from opensearchpy import OpenSearch

class MetricsExporter:
    def __init__(self, source_client: OpenSearch, metrics_client: OpenSearch, execution_id: str):
        self.source_client = source_client  # Client to get stats from
        self.metrics_client = metrics_client  # Client to export metrics to
        self.execution_id = execution_id
        self.metrics_index = f"benchmark-metrics-{time.strftime('%Y-%m')}-{execution_id}"
        self._ensure_index()
    
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
                    "number_of_replicas": 0,
                    "mapping.total_fields.limit": 2000
                }
            }
            self.metrics_client.indices.create(index=self.metrics_index, body=mapping)
    
    def export_node_stats(self, environment: str = "loadtest"):
        """Export node stats to metrics index"""
        try:
            stats = self.source_client.nodes.stats()
            timestamp = int(time.time() * 1000)
            
            for node_id, node_data in stats['nodes'].items():
                doc = self._flatten_node_stats(node_data)
                doc.update({
                    "@timestamp": timestamp,
                    "test-execution-id": self.execution_id,
                    "environment": environment,
                    "job": "opensearch-loadtest"
                })
                
                self.metrics_client.index(
                    index=self.metrics_index,
                    body=doc
                )
        except Exception as e:
            print(f"Failed to export node stats: {e}")
    
    def _flatten_node_stats(self, node_data: Dict[str, Any]) -> Dict[str, Any]:
        """Flatten nested node stats to match mapping structure"""
        flattened = {}
        
        # Process JVM stats
        if 'jvm' in node_data:
            jvm = node_data['jvm']
            
            # Memory stats
            if 'mem' in jvm:
                mem = jvm['mem']
                flattened.update({
                    'jvm_mem_heap_used_in_bytes': mem.get('heap_used_in_bytes'),
                    'jvm_mem_heap_max_in_bytes': mem.get('heap_max_in_bytes'),
                    'jvm_mem_heap_committed_in_bytes': mem.get('heap_committed_in_bytes'),
                    'jvm_mem_heap_used_percent': mem.get('heap_used_percent'),
                    'jvm_mem_non_heap_used_in_bytes': mem.get('non_heap_used_in_bytes'),
                    'jvm_mem_non_heap_committed_in_bytes': mem.get('non_heap_committed_in_bytes')
                })
                
                # Memory pools
                if 'pools' in mem:
                    for pool_name, pool_data in mem['pools'].items():
                        prefix = f"jvm_mem_pools_{pool_name.replace(' ', '_').lower()}"
                        flattened.update({
                            f"{prefix}_used_in_bytes": pool_data.get('used_in_bytes'),
                            f"{prefix}_max_in_bytes": pool_data.get('max_in_bytes'),
                            f"{prefix}_peak_used_in_bytes": pool_data.get('peak_used_in_bytes'),
                            f"{prefix}_peak_max_in_bytes": pool_data.get('peak_max_in_bytes')
                        })
            
            # GC stats
            if 'gc' in jvm and 'collectors' in jvm['gc']:
                for gc_name, gc_data in jvm['gc']['collectors'].items():
                    prefix = f"jvm_gc_collectors_{gc_name.replace(' ', '_').replace('-', '_')}"
                    flattened.update({
                        f"{prefix}_collection_count": gc_data.get('collection_count'),
                        f"{prefix}_collection_time_in_millis": gc_data.get('collection_time_in_millis')
                    })
        
        # Process thread pool stats
        if 'thread_pool' in node_data:
            for pool_name, pool_data in node_data['thread_pool'].items():
                prefix = f"thread_pool_{pool_name.replace('-', '_')}"
                flattened.update({
                    f"{prefix}_active": pool_data.get('active'),
                    f"{prefix}_threads": pool_data.get('threads'),
                    f"{prefix}_queue": pool_data.get('queue'),
                    f"{prefix}_completed": pool_data.get('completed'),
                    f"{prefix}_rejected": pool_data.get('rejected'),
                    f"{prefix}_largest": pool_data.get('largest')
                })
        
        # Process circuit breakers
        if 'breakers' in node_data:
            for breaker_name, breaker_data in node_data['breakers'].items():
                prefix = f"breakers_{breaker_name.replace('-', '_')}"
                flattened.update({
                    f"{prefix}_estimated_size_in_bytes": breaker_data.get('estimated_size_in_bytes'),
                    f"{prefix}_limit_size_in_bytes": breaker_data.get('limit_size_in_bytes'),
                    f"{prefix}_overhead": breaker_data.get('overhead'),
                    f"{prefix}_tripped": breaker_data.get('tripped')
                })
        
        # Process transport stats
        if 'transport' in node_data:
            transport = node_data['transport']
            flattened.update({
                'transport_server_open': transport.get('server_open'),
                'transport_rx_count': transport.get('rx_count'),
                'transport_rx_size_in_bytes': transport.get('rx_size_in_bytes'),
                'transport_tx_count': transport.get('tx_count'),
                'transport_tx_size_in_bytes': transport.get('tx_size_in_bytes'),
                'transport_total_outbound_connections': transport.get('total_outbound_connections')
            })
        
        # Process CPU stats
        if 'process' in node_data and 'cpu' in node_data['process']:
            cpu = node_data['process']['cpu']
            flattened.update({
                'process_cpu_percent': cpu.get('percent'),
                'process_cpu_total_in_millis': cpu.get('total_in_millis')
            })
        
        return {k: v for k, v in flattened.items() if v is not None}
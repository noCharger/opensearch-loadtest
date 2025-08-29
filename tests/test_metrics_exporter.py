import unittest
import sys
import os
from unittest.mock import Mock

# Add project root to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from src.utils.metrics_exporter import MetricsExporter
from src.utils.observability import ObservabilityMonitor
from src.utils.query_groups import QueryGroup

class TestMetricsExporter(unittest.TestCase):
    
    def setUp(self):
        self.source_client = Mock()
        self.metrics_client = Mock()
        self.execution_id = "test-123"
        self.observability_monitor = ObservabilityMonitor()
        
        # Mock indices.exists to return False so index creation is tested
        self.metrics_client.indices.exists.return_value = False
        
        self.exporter = MetricsExporter(
            self.source_client, 
            self.metrics_client, 
            self.execution_id,
            self.observability_monitor
        )
    
    def test_max_concurrency_included_in_export(self):
        # Simulate concurrent requests to build up max_concurrency
        self.observability_monitor.start_request("query1", QueryGroup.TERMS_AGGREGATION)
        self.observability_monitor.start_request("query2", QueryGroup.SORTING)
        self.observability_monitor.start_request("query3", QueryGroup.TEXT_QUERYING)
        
        # Mock nodes info and stats response
        mock_nodes_info = {
            'nodes': {
                'node1': {
                    'name': 'test-node-1',
                    'ip': '10.0.1.100',
                    'roles': ['data']
                }
            }
        }
        mock_stats = {
            'nodes': {
                'node1': {
                    'jvm': {'mem': {'heap_used_percent': 50}},
                    'process': {'cpu': {'percent': 30}}
                }
            }
        }
        self.source_client.nodes.info.return_value = mock_nodes_info
        self.source_client.nodes.stats.return_value = mock_stats
        
        # Export node stats
        self.exporter.export_node_stats()
        
        # Verify metrics were buffered (no immediate index call)
        self.metrics_client.index.assert_not_called()
        
        # Check that metrics are in the buffer
        self.assertEqual(len(self.exporter.pending_metrics), 2)  # index action + document
        doc = self.exporter.pending_metrics[1]
        self.assertEqual(doc['max_concurrency'], 3)
        self.assertEqual(doc['test-execution-id'], self.execution_id)
    
    def test_max_concurrency_zero_when_no_monitor(self):
        # Create exporter without observability monitor
        exporter_no_monitor = MetricsExporter(
            self.source_client, 
            self.metrics_client, 
            self.execution_id
        )
        
        # Mock nodes info and stats response
        mock_nodes_info = {
            'nodes': {
                'node1': {
                    'name': 'test-node-1',
                    'roles': ['data']
                }
            }
        }
        mock_stats = {
            'nodes': {
                'node1': {
                    'jvm': {'mem': {'heap_used_percent': 50}}
                }
            }
        }
        self.source_client.nodes.info.return_value = mock_nodes_info
        self.source_client.nodes.stats.return_value = mock_stats
        
        # Export node stats
        exporter_no_monitor.export_node_stats()
        
        # Verify max_concurrency is 0 when no monitor
        doc = exporter_no_monitor.pending_metrics[1]
        self.assertEqual(doc['max_concurrency'], 0)
    
    def test_export_query_metrics(self):
        # Set up max concurrency
        self.observability_monitor.start_request("test_query", QueryGroup.TERMS_AGGREGATION)
        
        # Export query metrics
        result = self.exporter.export_query_metrics("test_query", 150.5)
        
        # Verify export was successful
        self.assertTrue(result)
        
        # Verify metrics were buffered
        self.assertTrue(len(self.exporter.pending_metrics) >= 2)
        
        # Find the query metrics document in the buffer
        doc = None
        for i in range(1, len(self.exporter.pending_metrics), 2):
            if self.exporter.pending_metrics[i].get('query_name') == 'test_query':
                doc = self.exporter.pending_metrics[i]
                break
        
        self.assertIsNotNone(doc)
        self.assertEqual(doc['query_name'], "test_query")
        self.assertEqual(doc['query_latency'], 150.5)
        self.assertEqual(doc['query_max_concurrency'], 1)
        self.assertEqual(doc['total_max_concurrency'], 1)
        self.assertEqual(doc['test-execution-id'], self.execution_id)
    
    def test_bulk_upload_functionality(self):
        # Mock successful bulk response
        self.metrics_client.bulk.return_value = {'errors': False}
        
        # Add some metrics to buffer
        self.exporter.export_query_metrics("test_query1", 100.0)
        self.exporter.export_query_metrics("test_query2", 200.0)
        
        # Force bulk upload
        self.exporter._bulk_upload_metrics()
        
        # Verify bulk was called
        self.metrics_client.bulk.assert_called_once()
        
        # Verify buffer was cleared
        self.assertEqual(len(self.exporter.pending_metrics), 0)
    
    def test_max_concurrency_tracking(self):
        # Test that max_concurrency is properly tracked and retrieved
        self.observability_monitor.start_request("query1", QueryGroup.TERMS_AGGREGATION)
        self.observability_monitor.start_request("query2", QueryGroup.SORTING)
        self.observability_monitor.start_request("query3", QueryGroup.TEXT_QUERYING)
        
        # Verify max_concurrency is tracked
        self.assertEqual(self.observability_monitor.get_max_concurrency(), 3)
        
        # End some requests
        self.observability_monitor.end_request("query1")
        self.observability_monitor.end_request("query2")
        
        # Max should still be 3
        self.assertEqual(self.observability_monitor.get_max_concurrency(), 3)
        
        # Start a request for the test_query to build per-query max concurrency
        self.observability_monitor.start_request("test_query", QueryGroup.TERMS_AGGREGATION)
        
        # Test MetricsExporter uses the correct value
        result = self.exporter.export_query_metrics("test_query", 100.0)
        self.assertTrue(result)
        
        # Find the document in the buffer
        doc = None
        for i in range(1, len(self.exporter.pending_metrics), 2):
            if self.exporter.pending_metrics[i].get('query_name') == 'test_query':
                doc = self.exporter.pending_metrics[i]
                break
        
        self.assertIsNotNone(doc)
        self.assertEqual(doc['query_max_concurrency'], 1)  # Per-query max concurrency
        self.assertEqual(doc['total_max_concurrency'], 3)  # Total system max concurrency remains at peak
    
    def test_data_node_cpu_metrics_collection(self):
        """Test that CPU metrics are collected for data nodes"""
        # Mock nodes info and stats with CPU data
        mock_nodes_info = {
            'nodes': {
                'data_node_1': {
                    'name': 'data-node-1',
                    'ip': '10.0.1.100',
                    'roles': ['data', 'ingest']
                },
                'master_node_1': {
                    'name': 'master-node-1', 
                    'ip': '10.0.1.101',
                    'roles': ['master']
                }
            }
        }
        
        mock_nodes_stats = {
            'nodes': {
                'data_node_1': {
                    'jvm': {'mem': {'heap_used_percent': 45}},
                    'os': {
                        'cpu': {
                            'percent': 75,
                            'load_average': {'1m': 2.5, '5m': 2.1, '15m': 1.8}
                        },
                        'mem': {
                            'total_in_bytes': 16777216000,
                            'free_in_bytes': 4194304000,
                            'used_in_bytes': 12582912000,
                            'free_percent': 25,
                            'used_percent': 75
                        }
                    }
                },
                'master_node_1': {
                    'jvm': {'mem': {'heap_used_percent': 30}},
                    'os': {
                        'cpu': {
                            'percent': 15,
                            'load_average': {'1m': 0.5, '5m': 0.4, '15m': 0.3}
                        }
                    }
                }
            }
        }
        
        self.source_client.nodes.info.return_value = mock_nodes_info
        self.source_client.nodes.stats.return_value = mock_nodes_stats
        
        # Export node stats
        result = self.exporter.export_node_stats()
        self.assertTrue(result)
        
        # Verify we have metrics for data node only (master node is filtered out)
        self.assertEqual(len(self.exporter.pending_metrics), 2)  # 1 data node * 2 entries
        
        # Find data node document
        data_node_doc = self.exporter.pending_metrics[1]
        
        # Verify data node CPU metrics are present
        self.assertEqual(data_node_doc['node_type'], 'data')
        self.assertEqual(data_node_doc['node_name'], 'data-node-1')
        self.assertEqual(data_node_doc['os_cpu_percent'], 75)
        self.assertEqual(data_node_doc['os_cpu_load_average_1m'], 2.5)
        self.assertEqual(data_node_doc['os_cpu_load_average_5m'], 2.1)
        self.assertEqual(data_node_doc['os_cpu_load_average_15m'], 1.8)
        self.assertEqual(data_node_doc['os_mem_used_percent'], 75)
    
    def test_missing_cpu_data_handling(self):
        """Test handling when CPU data is missing from node stats"""
        mock_nodes_info = {
            'nodes': {
                'node_1': {
                    'name': 'test-node',
                    'roles': ['data']
                }
            }
        }
        
        # Node stats without OS/CPU data
        mock_nodes_stats = {
            'nodes': {
                'node_1': {
                    'jvm': {'mem': {'heap_used_percent': 50}}
                    # No 'os' section
                }
            }
        }
        
        self.source_client.nodes.info.return_value = mock_nodes_info
        self.source_client.nodes.stats.return_value = mock_nodes_stats
        
        # Should not fail even without CPU data
        result = self.exporter.export_node_stats()
        self.assertTrue(result)
        
        # Verify document was created but without CPU fields
        doc = self.exporter.pending_metrics[1]
        self.assertNotIn('os_cpu_percent', doc)
        self.assertNotIn('os_cpu_load_average_1m', doc)

if __name__ == '__main__':
    unittest.main()
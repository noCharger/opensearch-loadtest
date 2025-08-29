#!/usr/bin/env python3
import unittest
import tempfile
import os
import json
from analyze_logs import calculate_concurrency_per_query, load_logs, calculate_p90_per_second

class TestConcurrencyAnalysis(unittest.TestCase):
    
    def test_empty_metrics(self):
        """Test with empty metrics"""
        result = calculate_concurrency_per_query([])
        self.assertEqual(result, {})
    
    def test_single_query_no_overlap(self):
        """Test single query with no overlapping requests"""
        metrics = [
            {'query_name': 'test', 'timestamp': 10.0, 'duration': 1.0},
            {'query_name': 'test', 'timestamp': 15.0, 'duration': 1.0}
        ]
        result = calculate_concurrency_per_query(metrics)
        self.assertEqual(result['test'], 1)
    
    def test_single_query_with_overlap(self):
        """Test single query with overlapping requests"""
        metrics = [
            {'query_name': 'test', 'timestamp': 10.0, 'duration': 5.0},  # runs 5-10
            {'query_name': 'test', 'timestamp': 12.0, 'duration': 5.0}   # runs 7-12, overlaps at 7-10
        ]
        result = calculate_concurrency_per_query(metrics)
        self.assertEqual(result['test'], 2)
    
    def test_multiple_queries(self):
        """Test multiple query types with different max concurrency"""
        metrics = [
            {'query_name': 'query_a', 'timestamp': 10.0, 'duration': 3.0},
            {'query_name': 'query_a', 'timestamp': 11.0, 'duration': 3.0},
            {'query_name': 'query_a', 'timestamp': 12.0, 'duration': 3.0},
            {'query_name': 'query_b', 'timestamp': 20.0, 'duration': 2.0},
            {'query_name': 'query_b', 'timestamp': 21.0, 'duration': 2.0}
        ]
        result = calculate_concurrency_per_query(metrics)
        self.assertEqual(result['query_a'], 3)
        self.assertEqual(result['query_b'], 2)
    
    def test_max_concurrency_tracking(self):
        """Test that max concurrency is tracked correctly over time"""
        metrics = [
            {'query_name': 'test', 'timestamp': 10.0, 'duration': 5.0},  # 5-10
            {'query_name': 'test', 'timestamp': 12.0, 'duration': 5.0},  # 7-12
            {'query_name': 'test', 'timestamp': 14.0, 'duration': 5.0},  # 9-14
            {'query_name': 'test', 'timestamp': 20.0, 'duration': 1.0}   # 19-20, separate
        ]
        result = calculate_concurrency_per_query(metrics)
        self.assertEqual(result['test'], 3)  # Max of 3 concurrent at seconds 9-10
        
    def test_p90_calculation(self):
        """Test P90 latency calculation"""
        metrics = [
            {'query_name': 'test', 'timestamp': 10.0, 'duration': 100.0},
            {'query_name': 'test', 'timestamp': 10.5, 'duration': 200.0},
            {'query_name': 'test', 'timestamp': 10.8, 'duration': 300.0},
        ]
        result = calculate_p90_per_second(metrics)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['p90_latency'], 300.0)  # 90th percentile
        
    def test_load_logs_from_files(self):
        """Test loading logs from actual files"""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create test log file
            log_file = os.path.join(temp_dir, 'test123_query1.log')
            with open(log_file, 'w') as f:
                f.write(json.dumps({'event_type': 'SUCCESS', 'timestamp': 1000, 'duration': 150}) + '\n')
                f.write(json.dumps({'event_type': 'SUCCESS', 'timestamp': 1001, 'duration': 200}) + '\n')
            
            # Mock the logs directory
            original_cwd = os.getcwd()
            os.chdir(temp_dir)
            os.makedirs('logs', exist_ok=True)
            os.rename(log_file, os.path.join('logs', 'test123_query1.log'))
            
            try:
                metrics = load_logs('test123')
                self.assertEqual(len(metrics), 2)
                self.assertEqual(metrics[0]['query_name'], 'query1')
                self.assertEqual(metrics[0]['duration'], 150)
            finally:
                os.chdir(original_cwd)
    
    def test_analyze_data_node_cpu(self):
        """Test CPU analysis for data nodes"""
        from analyze_logs import analyze_data_node_cpu
        
        benchmark_metrics = [
            {
                'node_type': 'data',
                'node_name': 'data-node-1',
                'os_cpu_percent': 75.5,
                '@timestamp': 1000000
            },
            {
                'node_type': 'data', 
                'node_name': 'data-node-1',
                'os_cpu_percent': 80.2,
                '@timestamp': 2000000
            },
            {
                'node_type': 'data',
                'node_name': 'data-node-2', 
                'os_cpu_percent': 45.0,
                '@timestamp': 1000000
            },
            {
                'node_type': 'master',
                'node_name': 'master-node-1',
                'os_cpu_percent': 15.0,
                '@timestamp': 1000000
            }
        ]
        
        cpu_stats = analyze_data_node_cpu(benchmark_metrics)
        
        self.assertEqual(len(cpu_stats), 2)  # Only data nodes
        self.assertIn('data-node-1', cpu_stats)
        self.assertIn('data-node-2', cpu_stats)
        self.assertNotIn('master-node-1', cpu_stats)
        
        # Check data-node-1 stats
        node1_stats = cpu_stats['data-node-1']
        self.assertAlmostEqual(node1_stats['avg_cpu'], 77.85, places=1)
        self.assertEqual(node1_stats['max_cpu'], 80.2)
        self.assertEqual(node1_stats['min_cpu'], 75.5)
        self.assertEqual(node1_stats['samples'], 2)
        
        # Check data-node-2 stats
        node2_stats = cpu_stats['data-node-2']
        self.assertEqual(node2_stats['avg_cpu'], 45.0)
        self.assertEqual(node2_stats['samples'], 1)

if __name__ == '__main__':
    unittest.main()
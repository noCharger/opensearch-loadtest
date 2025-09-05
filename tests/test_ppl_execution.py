#!/usr/bin/env python3

import unittest
from unittest.mock import Mock, patch, MagicMock
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.loadtest.config import LoadTestConfig, QueryConfig, QueryType, LoadMode
from src.loadtest.load_tester import LoadTester

class TestPPLExecution(unittest.TestCase):
    """Test that PPL queries are executed correctly via PPL endpoint"""
    
    def setUp(self):
        """Set up test fixtures"""
        # Create a PPL query config
        self.ppl_query = QueryConfig(
            name="test_ppl_query",
            query_type=QueryType.PPL,
            query="source=big5* | stats count() by log.level",
            load_mode=LoadMode.CONCURRENCY,
            target_concurrency=1,
            index="big5*"
        )
        
        # Create a DSL query config for comparison
        self.dsl_query = QueryConfig(
            name="test_dsl_query",
            query_type=QueryType.DSL,
            query='{"query": {"match_all": {}}}',
            load_mode=LoadMode.CONCURRENCY,
            target_concurrency=1,
            index="big5*"
        )
        
        # Create load test config
        self.config = LoadTestConfig(
            host="localhost",
            port=9200,
            duration_seconds=1,
            queries=[self.ppl_query, self.dsl_query]
        )
    
    @patch('src.loadtest.load_tester.OpenSearch')
    def test_ppl_query_uses_ppl_endpoint(self, mock_opensearch_class):
        """Test that PPL queries use the /_plugins/_ppl endpoint"""
        # Setup mock client
        mock_client = Mock()
        mock_transport = Mock()
        mock_client.transport = mock_transport
        mock_client.info.return_value = {
            'version': {'number': '2.0.0'},
            'cluster_name': 'test-cluster'
        }
        mock_opensearch_class.return_value = mock_client
        
        # Create load tester
        load_tester = LoadTester(self.config)
        
        # Execute PPL query directly
        load_tester._execute_query(self.ppl_query)
        
        # Verify PPL endpoint was called
        mock_transport.perform_request.assert_called_once_with(
            'POST',
            '/_plugins/_ppl',
            body={"query": "source=big5* | stats count() by log.level"},
            timeout=300
        )
    
    @patch('src.loadtest.load_tester.OpenSearch')
    def test_dsl_query_uses_search_endpoint(self, mock_opensearch_class):
        """Test that DSL queries use the search endpoint"""
        # Setup mock client
        mock_client = Mock()
        mock_client.info.return_value = {
            'version': {'number': '2.0.0'},
            'cluster_name': 'test-cluster'
        }
        mock_opensearch_class.return_value = mock_client
        
        # Create load tester
        load_tester = LoadTester(self.config)
        
        # Execute DSL query directly
        load_tester._execute_query(self.dsl_query)
        
        # Verify search endpoint was called
        mock_client.search.assert_called_once_with(
            index="big5*",
            body={"query": {"match_all": {}}},
            timeout=300
        )
    
    @patch('src.loadtest.load_tester.OpenSearch')
    def test_query_type_detection(self, mock_opensearch_class):
        """Test that query types are correctly detected and routed"""
        # Setup mock client
        mock_client = Mock()
        mock_transport = Mock()
        mock_client.transport = mock_transport
        mock_client.info.return_value = {
            'version': {'number': '2.0.0'},
            'cluster_name': 'test-cluster'
        }
        mock_opensearch_class.return_value = mock_client
        
        # Create load tester
        load_tester = LoadTester(self.config)
        
        # Test PPL query routing
        self.assertEqual(self.ppl_query.query_type, QueryType.PPL)
        load_tester._execute_query(self.ppl_query)
        mock_transport.perform_request.assert_called_with(
            'POST',
            '/_plugins/_ppl',
            body={"query": "source=big5* | stats count() by log.level"},
            timeout=300
        )
        
        # Test DSL query routing
        self.assertEqual(self.dsl_query.query_type, QueryType.DSL)
        load_tester._execute_query(self.dsl_query)
        mock_client.search.assert_called_with(
            index="big5*",
            body={"query": {"match_all": {}}},
            timeout=300
        )
    
    @patch('src.loadtest.load_tester.OpenSearch')
    def test_ppl_query_body_format(self, mock_opensearch_class):
        """Test that PPL query body is formatted correctly"""
        # Setup mock client
        mock_client = Mock()
        mock_transport = Mock()
        mock_client.transport = mock_transport
        mock_client.info.return_value = {
            'version': {'number': '2.0.0'},
            'cluster_name': 'test-cluster'
        }
        mock_opensearch_class.return_value = mock_client
        
        # Create different PPL queries
        ppl_queries = [
            QueryConfig(
                name="simple_search",
                query_type=QueryType.PPL,
                query="source=big5* | head 10",
                load_mode=LoadMode.CONCURRENCY,
                target_concurrency=1
            ),
            QueryConfig(
                name="aggregation",
                query_type=QueryType.PPL,
                query="source=big5* | stats count() by log.level | sort count desc",
                load_mode=LoadMode.CONCURRENCY,
                target_concurrency=1
            ),
            QueryConfig(
                name="filtering",
                query_type=QueryType.PPL,
                query="source=big5* | where log.level='ERROR' | fields @timestamp, message",
                load_mode=LoadMode.CONCURRENCY,
                target_concurrency=1
            )
        ]
        
        load_tester = LoadTester(self.config)
        
        # Test each PPL query
        for ppl_query in ppl_queries:
            mock_transport.reset_mock()
            load_tester._execute_query(ppl_query)
            
            # Verify correct PPL endpoint and body format
            mock_transport.perform_request.assert_called_once_with(
                'POST',
                '/_plugins/_ppl',
                body={"query": ppl_query.query},
                timeout=300
            )
    
    def test_query_type_enum_values(self):
        """Test that QueryType enum has correct values"""
        self.assertEqual(QueryType.PPL.value, "ppl")
        self.assertEqual(QueryType.DSL.value, "dsl")
    
    @patch('src.loadtest.load_tester.OpenSearch')
    def test_mixed_query_execution(self, mock_opensearch_class):
        """Test that mixed PPL and DSL queries are executed correctly"""
        # Setup mock client
        mock_client = Mock()
        mock_transport = Mock()
        mock_client.transport = mock_transport
        mock_client.info.return_value = {
            'version': {'number': '2.0.0'},
            'cluster_name': 'test-cluster'
        }
        mock_opensearch_class.return_value = mock_client
        
        # Create config with mixed queries
        mixed_config = LoadTestConfig(
            host="localhost",
            port=9200,
            duration_seconds=1,
            queries=[self.ppl_query, self.dsl_query]
        )
        
        load_tester = LoadTester(mixed_config)
        
        # Execute both queries
        load_tester._execute_query(self.ppl_query)
        load_tester._execute_query(self.dsl_query)
        
        # Verify PPL query used PPL endpoint
        mock_transport.perform_request.assert_called_with(
            'POST',
            '/_plugins/_ppl',
            body={"query": "source=big5* | stats count() by log.level"},
            timeout=300
        )
        
        # Verify DSL query used search endpoint
        mock_client.search.assert_called_with(
            index="big5*",
            body={"query": {"match_all": {}}},
            timeout=300
        )

if __name__ == '__main__':
    unittest.main()
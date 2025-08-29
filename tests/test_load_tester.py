#!/usr/bin/env python3
import unittest
import sys
import os
from unittest.mock import Mock, patch
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from src.loadtest.load_tester import LoadTester
from src.loadtest.config import LoadTestConfig
from src.utils.query_groups import QueryGroup

class TestLoadTester(unittest.TestCase):
    
    def setUp(self):
        self.config = LoadTestConfig(
            host='localhost',
            port=9200,
            username='admin',
            password='admin',
            duration=10,
            queries=['test_query']
        )
        self.mock_client = Mock()
        
    @patch('src.loadtest.load_tester.OpenSearch')
    def test_initialization(self, mock_opensearch):
        mock_opensearch.return_value = self.mock_client
        tester = LoadTester(self.config)
        self.assertEqual(tester.config, self.config)
        self.assertIsNotNone(tester.client)
        
    @patch('src.loadtest.load_tester.OpenSearch')
    def test_execute_query_success(self, mock_opensearch):
        mock_opensearch.return_value = self.mock_client
        self.mock_client.transport.perform_request.return_value = {'hits': {'total': {'value': 100}}}
        
        tester = LoadTester(self.config)
        result = tester._execute_query('source=test | head 10', 'test_query')
        
        self.assertTrue(result)
        self.mock_client.transport.perform_request.assert_called_once()
        
    @patch('src.loadtest.load_tester.OpenSearch')
    def test_execute_query_failure(self, mock_opensearch):
        mock_opensearch.return_value = self.mock_client
        self.mock_client.transport.perform_request.side_effect = Exception("Query failed")
        
        tester = LoadTester(self.config)
        result = tester._execute_query('invalid query', 'test_query')
        
        self.assertFalse(result)
        
    @patch('src.loadtest.load_tester.OpenSearch')
    def test_query_group_assignment(self, mock_opensearch):
        mock_opensearch.return_value = self.mock_client
        tester = LoadTester(self.config)
        
        # Test different query patterns
        self.assertEqual(tester._get_query_group('source=test | sort timestamp'), QueryGroup.SORTING)
        self.assertEqual(tester._get_query_group('source=test | stats count()'), QueryGroup.TERMS_AGGREGATION)
        self.assertEqual(tester._get_query_group('source=test | where message="error"'), QueryGroup.TEXT_QUERYING)
        self.assertEqual(tester._get_query_group('source=test | where timestamp > "2023-01-01"'), QueryGroup.RANGE_QUERIES)
        self.assertEqual(tester._get_query_group('source=test | head 10'), QueryGroup.MATH_FUNCTIONS)

if __name__ == '__main__':
    unittest.main()
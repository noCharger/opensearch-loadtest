#!/usr/bin/env python3
import unittest
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from src.loadtest.config import LoadTestConfig

class TestLoadTestConfig(unittest.TestCase):
    
    def test_basic_config_creation(self):
        config = LoadTestConfig(
            host='localhost',
            port=9200,
            username='admin',
            password='admin',
            duration=300,
            queries=['test_query']
        )
        
        self.assertEqual(config.host, 'localhost')
        self.assertEqual(config.port, 9200)
        self.assertEqual(config.duration, 300)
        self.assertEqual(len(config.queries), 1)
        
    def test_ssl_config(self):
        config = LoadTestConfig(
            host='search.example.com',
            port=443,
            username='admin',
            password='admin',
            duration=300,
            queries=['test_query'],
            use_ssl=True
        )
        
        self.assertTrue(config.use_ssl)
        
    def test_default_values(self):
        config = LoadTestConfig(
            host='localhost',
            port=9200,
            username='admin',
            password='admin',
            duration=300,
            queries=['test_query']
        )
        
        self.assertFalse(config.use_ssl)
        self.assertEqual(config.concurrency, 1)
        
    def test_custom_concurrency(self):
        config = LoadTestConfig(
            host='localhost',
            port=9200,
            username='admin',
            password='admin',
            duration=300,
            queries=['test_query'],
            concurrency=10
        )
        
        self.assertEqual(config.concurrency, 10)
        
    def test_empty_queries_list(self):
        with self.assertRaises(ValueError):
            LoadTestConfig(
                host='localhost',
                port=9200,
                username='admin',
                password='admin',
                duration=300,
                queries=[]
            )
            
    def test_invalid_duration(self):
        with self.assertRaises(ValueError):
            LoadTestConfig(
                host='localhost',
                port=9200,
                username='admin',
                password='admin',
                duration=0,
                queries=['test_query']
            )

if __name__ == '__main__':
    unittest.main()
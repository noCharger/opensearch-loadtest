#!/usr/bin/env python3

import unittest
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.utils.production_config import ProductionLoadConfig
from src.utils.query_groups import QueryGroup, QueryGroupMapper
from src.loadtest.config import QueryConfig, QueryType, LoadMode
from src.utils.query_loader import QueryLoader

class TestSingleQueryConfig(unittest.TestCase):
    """Test single query configuration for all available target queries"""
    
    def test_all_target_queries_single_query_config(self):
        """Test single query config for each available target query"""
        # Available target queries from run_ppl_load_test.py
        target_queries = ['composite_terms', 'desc_sort_timestamp', 'range', 'default', 'term', 'date_histogram_hourly_agg', 'keyword_terms']
        duration = 3600
        ramp_step = 10
        
        # Load PPL queries
        ppl_query_names = ["composite_terms", "desc_sort_timestamp", "range", "default", "term"]
        queries = QueryLoader.load_specific_queries(ppl_query_names, "queries", "big5*")
        
        for target_query in target_queries:
            with self.subTest(target_query=target_query):
                # Skip queries not in our test set
                if target_query not in ppl_query_names:
                    continue
                    
                config_dict = ProductionLoadConfig.get_single_query_power2_ramp_config(
                    target_query, ramp_step, duration
                )
                
                # Apply config to queries
                ProductionLoadConfig.apply_single_query_config_to_queries(queries, config_dict, target_query, duration)
                
                # Verify target query gets power-of-2 ramp
                target_query_obj = next((q for q in queries if q.name == target_query), None)
                other_queries = [q for q in queries if q.name != target_query]
                
                # Target query should have power-of-2 ramp
                self.assertIsNotNone(target_query_obj, f"Target query {target_query} not found")
                self.assertEqual(target_query_obj.load_mode, LoadMode.CONCURRENCY)
                self.assertGreater(len(target_query_obj.target_concurrency), 1)
                self.assertEqual(target_query_obj.target_concurrency[0].concurrency, 1)
                
                # Other queries should have zero load (excluded from test)
                for query in other_queries:
                    self.assertEqual(query.load_mode, LoadMode.CONCURRENCY)
                    self.assertEqual(len(query.target_concurrency), 1)
                    self.assertEqual(query.target_concurrency[0].concurrency, 0)
                    self.assertEqual(query.target_concurrency[0].duration_seconds, 1)
    
    def test_single_query_power2_ramp_pattern(self):
        """Test that single query power-of-2 ramp follows correct pattern"""
        target_query = "desc_sort_timestamp"
        duration = 7200  # 2 hours
        ramp_step = 10   # 10 minutes
        
        config_dict = ProductionLoadConfig.get_single_query_power2_ramp_config(
            target_query, ramp_step, duration
        )
        
        # Should only contain target query
        self.assertEqual(len(config_dict), 1)
        self.assertIn(target_query, config_dict)
        
        # Verify power-of-2 ramp pattern
        query_config = config_dict[target_query]
        self.assertEqual(query_config["load_mode"], LoadMode.CONCURRENCY)
        ramp = query_config["target_concurrency"]
        
        # Should have multiple steps
        self.assertGreater(len(ramp), 1)
        
        # First step should be 1
        self.assertEqual(ramp[0].concurrency, 1)
        
        # Each step should be 10 minutes (600 seconds)
        for step in ramp:
            self.assertEqual(step.duration_seconds, 600)
    
    def test_conservative_ramp_for_special_queries(self):
        """Test that date_histogram and keyword_terms queries use conservative ramp"""
        duration = 7200  # 2 hours
        ramp_step = 10   # 10 minutes
        
        # Test date_histogram_hourly_agg
        config_dict = ProductionLoadConfig.get_single_query_power2_ramp_config(
            "date_histogram_hourly_agg", ramp_step, duration
        )
        
        query_config = config_dict["date_histogram_hourly_agg"]
        ramp = query_config["target_concurrency"]
        
        # Verify conservative ramp pattern: 1, 2, 4, 6, 8, 10, 12...
        expected_pattern = [1, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22]
        for i, step in enumerate(ramp[:12]):
            self.assertEqual(step.concurrency, expected_pattern[i], 
                           f"Step {i+1}: expected {expected_pattern[i]}, got {step.concurrency}")
        
        # Test keyword_terms
        config_dict = ProductionLoadConfig.get_single_query_power2_ramp_config(
            "keyword_terms", ramp_step, duration
        )
        
        query_config = config_dict["keyword_terms"]
        ramp = query_config["target_concurrency"]
        
        # Should follow same conservative pattern
        for i, step in enumerate(ramp[:12]):
            self.assertEqual(step.concurrency, expected_pattern[i], 
                           f"Step {i+1}: expected {expected_pattern[i]}, got {step.concurrency}")
    
    def test_power2_ramp_for_regular_queries(self):
        """Test that regular queries still use power-of-2 ramp"""
        duration = 7200  # 2 hours
        ramp_step = 10   # 10 minutes
        
        regular_queries = ["range", "default", "term", "composite_terms", "desc_sort_timestamp"]
        
        for query_name in regular_queries:
            with self.subTest(query_name=query_name):
                config_dict = ProductionLoadConfig.get_single_query_power2_ramp_config(
                    query_name, ramp_step, duration
                )
                
                query_config = config_dict[query_name]
                ramp = query_config["target_concurrency"]
                
                # Verify power-of-2 pattern: 1, 2, 4, 8, 16, 32, 64...
                expected_power2 = [1, 2, 4, 8, 16, 32, 64]
                for i, step in enumerate(ramp[:7]):
                    self.assertEqual(step.concurrency, expected_power2[i], 
                                   f"Query {query_name} Step {i+1}: expected {expected_power2[i]}, got {step.concurrency}")
    
    def test_single_query_isolation(self):
        """Test that only target query runs, others have minimal load"""
        target_query = "composite_terms"
        duration = 3600
        
        # Create test queries
        queries = [
            QueryConfig(
                name="composite_terms",
                query_type=QueryType.PPL,
                query="source=big5* | stats count() by log.level",
                load_mode=LoadMode.CONCURRENCY,
                target_concurrency=1
            ),
            QueryConfig(
                name="desc_sort_timestamp",
                query_type=QueryType.PPL,
                query="source=big5* | sort @timestamp desc",
                load_mode=LoadMode.CONCURRENCY,
                target_concurrency=1
            ),
            QueryConfig(
                name="range",
                query_type=QueryType.PPL,
                query="source=big5* | where @timestamp > '2023-01-01'",
                load_mode=LoadMode.CONCURRENCY,
                target_concurrency=1
            ),
            QueryConfig(
                name="default",
                query_type=QueryType.PPL,
                query="source=big5*",
                load_mode=LoadMode.CONCURRENCY,
                target_concurrency=1
            ),
            QueryConfig(
                name="term",
                query_type=QueryType.PPL,
                query="source=big5* | where log.level='ERROR'",
                load_mode=LoadMode.CONCURRENCY,
                target_concurrency=1
            )
        ]
        
        # Apply single query config
        config_dict = ProductionLoadConfig.get_single_query_power2_ramp_config(
            target_query, 10, duration
        )
        ProductionLoadConfig.apply_single_query_config_to_queries(
            queries, config_dict, target_query, duration
        )
        
        # Count queries with significant load vs minimal load
        high_load_queries = []
        minimal_load_queries = []
        
        for query in queries:
            if query.name == target_query:
                # Target query should have power-of-2 ramp
                self.assertGreater(len(query.target_concurrency), 1)
                high_load_queries.append(query.name)
            else:
                # Other queries should have zero load (excluded from test)
                self.assertEqual(len(query.target_concurrency), 1)
                self.assertEqual(query.target_concurrency[0].concurrency, 0)
                minimal_load_queries.append(query.name)
        
        # Verify isolation: only 1 query with high load, 4 with zero load
        self.assertEqual(len(high_load_queries), 1)
        self.assertEqual(len(minimal_load_queries), 4)
        self.assertEqual(high_load_queries[0], target_query)
    
    def test_query_group_mapping_for_target_queries(self):
        """Test that all target queries have proper group mappings"""
        target_queries = ['composite_terms', 'desc_sort_timestamp', 'range', 'default', 'term', 'date_histogram_hourly_agg', 'keyword_terms']
        
        expected_groups = {
            'composite_terms': QueryGroup.TERMS_AGGREGATION,
            'desc_sort_timestamp': QueryGroup.SORTING,
            'range': QueryGroup.RANGE_QUERIES,
            'default': QueryGroup.TEXT_QUERYING,
            'term': QueryGroup.TEXT_QUERYING,
            'date_histogram_hourly_agg': QueryGroup.DATE_HISTOGRAM,
            'keyword_terms': QueryGroup.TERMS_AGGREGATION
        }
        
        for query_name, expected_group in expected_groups.items():
            with self.subTest(query_name=query_name):
                actual_group = QueryGroupMapper.get_group(query_name)
                self.assertEqual(actual_group, expected_group)

if __name__ == '__main__':
    unittest.main()
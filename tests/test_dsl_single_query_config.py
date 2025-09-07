#!/usr/bin/env python3

import unittest
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.utils.production_config import ProductionLoadConfig
from src.utils.query_groups import QueryGroup, QueryGroupMapper
from src.loadtest.config import QueryConfig, QueryType, LoadMode
from src.utils.query_loader import QueryLoader

class TestDSLSingleQueryConfig(unittest.TestCase):
    """Test DSL single query configuration for all available target queries"""
    
    def test_dsl_target_queries_single_query_config(self):
        """Test DSL single query config for each available target query"""
        # Available DSL target queries from run_dsl_load_test.py
        target_queries = ['match-all', 'term', 'range', 'range-numeric', 'composite-terms', 'desc_sort_timestamp', 'date_histogram_hourly_agg', 'keyword-terms']
        duration = 3600
        ramp_step = 10
        
        # Load DSL queries
        dsl_queries = QueryLoader.load_dsl_queries("queries/dsl_queries.json", "big5*")
        
        for target_query in target_queries:
            with self.subTest(target_query=target_query):
                # Skip queries not in our DSL set
                if not any(q.name == target_query for q in dsl_queries):
                    continue
                    
                config_dict = ProductionLoadConfig.get_single_query_power2_ramp_config(
                    target_query, ramp_step, duration
                )
                
                # Apply config to queries
                ProductionLoadConfig.apply_single_query_config_to_queries(dsl_queries, config_dict, target_query, duration)
                
                # Verify target query gets power-of-2 ramp
                target_query_obj = next((q for q in dsl_queries if q.name == target_query), None)
                other_queries = [q for q in dsl_queries if q.name != target_query]
                
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
    
    def test_dsl_conservative_ramp_for_special_queries(self):
        """Test that DSL date_histogram and keyword-terms queries use conservative ramp"""
        duration = 7200  # 2 hours
        ramp_step = 10   # 10 minutes
        
        # Test date_histogram_hourly_agg (conservative)
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
        
        # Test keyword-terms (conservative)
        config_dict = ProductionLoadConfig.get_single_query_power2_ramp_config(
            "keyword-terms", ramp_step, duration
        )
        
        query_config = config_dict["keyword-terms"]
        ramp = query_config["target_concurrency"]
        
        # Should follow same conservative pattern
        for i, step in enumerate(ramp[:12]):
            self.assertEqual(step.concurrency, expected_pattern[i], 
                           f"Step {i+1}: expected {expected_pattern[i]}, got {step.concurrency}")
    
    def test_dsl_power2_ramp_for_regular_queries(self):
        """Test that regular DSL queries use power-of-2 ramp"""
        duration = 7200  # 2 hours
        ramp_step = 10   # 10 minutes
        
        regular_queries = ["range", "match-all", "term", "composite-terms", "desc_sort_timestamp", "range-numeric"]
        
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
    
    def test_dsl_single_query_isolation(self):
        """Test that only target DSL query runs, others have zero load"""
        target_query = "match-all"
        duration = 3600
        
        # Load DSL queries
        dsl_queries = QueryLoader.load_dsl_queries("queries/dsl_queries.json", "big5*")
        
        # Apply single query config
        config_dict = ProductionLoadConfig.get_single_query_power2_ramp_config(
            target_query, 10, duration
        )
        ProductionLoadConfig.apply_single_query_config_to_queries(
            dsl_queries, config_dict, target_query, duration
        )
        
        # Count queries with significant load vs zero load
        high_load_queries = []
        zero_load_queries = []
        
        for query in dsl_queries:
            if query.name == target_query:
                # Target query should have power-of-2 ramp
                self.assertGreater(len(query.target_concurrency), 1)
                high_load_queries.append(query.name)
            else:
                # Other queries should have zero load
                self.assertEqual(len(query.target_concurrency), 1)
                self.assertEqual(query.target_concurrency[0].concurrency, 0)
                zero_load_queries.append(query.name)
        
        # Verify isolation: only 1 query with high load, all others with zero load
        self.assertEqual(len(high_load_queries), 1)
        self.assertGreater(len(zero_load_queries), 0)
        self.assertEqual(high_load_queries[0], target_query)
    
    def test_dsl_query_group_mappings(self):
        """Test that DSL target queries have proper group mappings"""
        target_queries = ['match-all', 'term', 'range', 'range-numeric', 'composite-terms', 'desc_sort_timestamp', 'date_histogram_hourly_agg', 'keyword-terms']
        
        expected_groups = {
            'match-all': QueryGroup.TEXT_QUERYING,
            'term': QueryGroup.TEXT_QUERYING,
            'range': QueryGroup.RANGE_QUERIES,
            'range-numeric': QueryGroup.RANGE_QUERIES,
            'composite-terms': QueryGroup.TERMS_AGGREGATION,
            'desc_sort_timestamp': QueryGroup.SORTING,
            'date_histogram_hourly_agg': QueryGroup.DATE_HISTOGRAM,
            'keyword-terms': QueryGroup.TERMS_AGGREGATION
        }
        
        for query_name, expected_group in expected_groups.items():
            with self.subTest(query_name=query_name):
                actual_group = QueryGroupMapper.get_group(query_name)
                self.assertEqual(actual_group, expected_group)

if __name__ == '__main__':
    unittest.main()
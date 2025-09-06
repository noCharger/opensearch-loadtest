#!/usr/bin/env python3

import unittest
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.utils.production_config import ProductionLoadConfig
from src.utils.query_groups import QueryGroup, QueryGroupMapper
from src.loadtest.config import QueryConfig, QueryType, LoadMode
from src.utils.query_loader import QueryLoader

class TestDSLSingleGroupConfig(unittest.TestCase):
    """Test DSL single group configuration for all query groups"""
    
    def test_dsl_all_query_groups_single_group_config(self):
        """Test DSL single group config for each query group"""
        duration = 3600
        ramp_step = 10
        
        # Load DSL queries
        dsl_queries = QueryLoader.load_dsl_queries("queries/dsl_queries.json", "big5*")
        
        for target_group in QueryGroup:
            with self.subTest(target_group=target_group):
                config_dict = ProductionLoadConfig.get_single_group_power2_ramp_config(
                    target_group, ramp_step, duration
                )
                
                # Apply config to DSL queries
                ProductionLoadConfig.apply_single_group_config_to_queries(dsl_queries, config_dict, target_group)
                
                # Verify target group queries get power-of-2 ramp
                target_queries = [q for q in dsl_queries if q.query_group == target_group]
                other_queries = [q for q in dsl_queries if q.query_group != target_group]
                
                # Target group should have queries with power-of-2 ramp
                for query in target_queries:
                    self.assertEqual(query.load_mode, LoadMode.CONCURRENCY)
                    self.assertGreater(len(query.target_concurrency), 1)
                    self.assertEqual(query.target_concurrency[0].concurrency, 1)
                
                # Other groups should have zero concurrency
                for query in other_queries:
                    self.assertEqual(query.load_mode, LoadMode.CONCURRENCY)
                    self.assertEqual(len(query.target_concurrency), 1)
                    self.assertEqual(query.target_concurrency[0].concurrency, 0)
    
    def test_dsl_query_group_mappings(self):
        """Test that DSL queries are properly mapped to query groups"""
        # Load DSL queries
        dsl_queries = QueryLoader.load_dsl_queries("queries/dsl_queries.json", "big5*")
        
        # Verify each group has at least one DSL query
        group_counts = {}
        for query in dsl_queries:
            group = query.query_group
            group_counts[group] = group_counts.get(group, 0) + 1
        
        # All 5 groups should have DSL queries
        self.assertEqual(len(group_counts), 5)
        for group in QueryGroup:
            self.assertIn(group, group_counts)
            self.assertGreater(group_counts[group], 0)
    
    def test_specific_dsl_query_mappings(self):
        """Test specific DSL query name mappings"""
        test_cases = [
            ("match-all", QueryGroup.TEXT_QUERYING),
            ("term", QueryGroup.TEXT_QUERYING),
            ("keyword-in-range", QueryGroup.TEXT_QUERYING),
            ("desc_sort_timestamp", QueryGroup.SORTING),
            ("sort_numeric_desc", QueryGroup.SORTING),
            ("range", QueryGroup.RANGE_QUERIES),
            ("range-numeric", QueryGroup.RANGE_QUERIES),
            ("date_histogram_hourly_agg", QueryGroup.DATE_HISTOGRAM),
            ("composite-date_histogram-daily", QueryGroup.DATE_HISTOGRAM),
            ("composite-terms", QueryGroup.TERMS_AGGREGATION),
            ("terms-significant-1", QueryGroup.TERMS_AGGREGATION),
        ]
        
        for query_name, expected_group in test_cases:
            with self.subTest(query_name=query_name):
                actual_group = QueryGroupMapper.get_group(query_name)
                self.assertEqual(actual_group, expected_group)

if __name__ == '__main__':
    unittest.main()
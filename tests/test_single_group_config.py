#!/usr/bin/env python3

import unittest
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.utils.production_config import ProductionLoadConfig
from src.utils.query_groups import QueryGroup, QueryGroupMapper
from src.loadtest.config import QueryConfig, QueryType, LoadMode

class TestSingleGroupConfig(unittest.TestCase):
    """Test single group exponential configuration"""
    
    def test_single_group_exponential_timeline(self):
        """Test that only target group gets exponential load, others get minimal load"""
        target_group = QueryGroup.SORTING
        duration = 3600
        ramp_step = 10
        
        # Get single group exponential config
        config_dict = ProductionLoadConfig.get_single_group_exponential_config(
            target_group, ramp_step, duration
        )
        
        # Verify target group gets exponential ramp
        target_config = config_dict[target_group]
        self.assertEqual(target_config["load_mode"], LoadMode.CONCURRENCY)
        target_ramp = target_config["target_concurrency"]
        
        # Should have multiple steps with exponential growth
        self.assertGreater(len(target_ramp), 1)
        self.assertEqual(target_ramp[0].concurrency, 1)  # Start at 1
        self.assertEqual(target_ramp[-1].concurrency, 64)  # End at 64
        
        # Verify other groups get zero load (excluded)
        for group, group_config in config_dict.items():
            if group != target_group:
                other_ramp = group_config["target_concurrency"]
                self.assertEqual(len(other_ramp), 1)  # Single step
                self.assertEqual(other_ramp[0].concurrency, 0)  # Zero concurrency
                self.assertEqual(other_ramp[0].duration_seconds, 1)  # Minimal duration
    
    def test_single_group_timeline_verification(self):
        """Test timeline shows 0 for non-target groups"""
        target_group = QueryGroup.SORTING
        
        # Create mock queries for different groups
        queries = [
            QueryConfig(
                name="desc_sort_timestamp",
                query_type=QueryType.PPL,
                query="source=big5* | sort @timestamp desc",
                load_mode=LoadMode.CONCURRENCY,
                target_concurrency=1
            ),
            QueryConfig(
                name="composite_terms", 
                query_type=QueryType.PPL,
                query="source=big5* | stats count() by log.level",
                load_mode=LoadMode.CONCURRENCY,
                target_concurrency=1
            ),
            QueryConfig(
                name="range",
                query_type=QueryType.PPL,
                query="source=big5* | where @timestamp > '2023-01-01'",
                load_mode=LoadMode.CONCURRENCY,
                target_concurrency=1
            )
        ]
        
        # Assign query groups
        for query in queries:
            query.query_group = QueryGroupMapper.get_group(query.name)
        
        # Apply single group config
        config_dict = ProductionLoadConfig.get_single_group_exponential_config(
            target_group, 10, 3600
        )
        ProductionLoadConfig.apply_single_group_config_to_queries(
            queries, config_dict, target_group
        )
        
        # Verify only SORTING group query gets exponential ramp
        for query in queries:
            if query.query_group == QueryGroup.SORTING:
                # Target group should have exponential ramp
                self.assertGreater(len(query.target_concurrency), 1)
                self.assertEqual(query.target_concurrency[0].concurrency, 1)
                self.assertEqual(query.target_concurrency[-1].concurrency, 64)
            else:
                # Other groups should have zero load (excluded)
                self.assertEqual(len(query.target_concurrency), 1)
                self.assertEqual(query.target_concurrency[0].concurrency, 0)
    
    def test_single_query_exponential_config(self):
        """Test single query exponential configuration"""
        target_query = "composite_terms"
        duration = 3600
        ramp_step = 10
        
        # Get single query exponential config
        config_dict = ProductionLoadConfig.get_single_query_exponential_config(
            target_query, ramp_step, duration
        )
        
        # Should only contain target query
        self.assertEqual(len(config_dict), 1)
        self.assertIn(target_query, config_dict)
        
        # Verify exponential ramp
        query_config = config_dict[target_query]
        self.assertEqual(query_config["load_mode"], LoadMode.CONCURRENCY)
        ramp = query_config["target_concurrency"]
        
        self.assertGreater(len(ramp), 1)
        self.assertEqual(ramp[0].concurrency, 1)
        self.assertEqual(ramp[-1].concurrency, 64)
    
    def test_apply_single_query_config(self):
        """Test applying single query config leaves others at minimal load"""
        target_query = "composite_terms"
        duration = 3600
        
        # Create mock queries
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
            )
        ]
        
        # Apply single query config
        config_dict = ProductionLoadConfig.get_single_query_exponential_config(
            target_query, 10, duration
        )
        ProductionLoadConfig.apply_single_query_config_to_queries(
            queries, config_dict, target_query, duration
        )
        
        # Verify only target query gets exponential ramp
        for query in queries:
            if query.name == target_query:
                # Target query should have exponential ramp
                self.assertGreater(len(query.target_concurrency), 1)
                self.assertEqual(query.target_concurrency[0].concurrency, 1)
                self.assertEqual(query.target_concurrency[-1].concurrency, 64)
            else:
                # Other queries should have minimal load (single query test keeps others at 1)
                self.assertEqual(len(query.target_concurrency), 1)
                self.assertEqual(query.target_concurrency[0].concurrency, 1)
                self.assertEqual(query.target_concurrency[0].duration_seconds, duration)
    
    def test_timeline_concurrency_at_different_times(self):
        """Test concurrency values at different time points"""
        target_group = QueryGroup.SORTING
        duration = 3600
        ramp_step = 10
        
        config_dict = ProductionLoadConfig.get_single_group_exponential_config(
            target_group, ramp_step, duration
        )
        
        # Get target group ramp
        target_ramp = config_dict[target_group]["target_concurrency"]
        
        # Test concurrency at different time points
        time_points = [0, 600, 1200, 1800, 2400, 3000, 3600]
        
        for time_point in time_points:
            cumulative_time = 0
            found_concurrency = None
            
            for ramp in target_ramp:
                if time_point <= cumulative_time + ramp.duration_seconds:
                    found_concurrency = ramp.concurrency
                    break
                cumulative_time += ramp.duration_seconds
            
            if found_concurrency is None:
                found_concurrency = target_ramp[-1].concurrency
            
            # At time 0, should be 1
            if time_point == 0:
                self.assertEqual(found_concurrency, 1)
            # At end time, should be 64
            elif time_point == 3600:
                self.assertEqual(found_concurrency, 64)
            # Should be increasing over time
            self.assertGreaterEqual(found_concurrency, 1)
            self.assertLessEqual(found_concurrency, 64)
    
    def test_all_query_groups_single_group_config(self):
        """Test single group config for each query group"""
        duration = 3600
        ramp_step = 10
        
        for target_group in QueryGroup:
            with self.subTest(target_group=target_group):
                config_dict = ProductionLoadConfig.get_single_group_exponential_config(
                    target_group, ramp_step, duration
                )
                
                # Target group gets exponential ramp
                target_config = config_dict[target_group]
                self.assertEqual(target_config["load_mode"], LoadMode.CONCURRENCY)
                target_ramp = target_config["target_concurrency"]
                self.assertGreater(len(target_ramp), 1)
                self.assertEqual(target_ramp[0].concurrency, 1)
                self.assertEqual(target_ramp[-1].concurrency, 64)
                
                # All other groups get zero load
                for group, group_config in config_dict.items():
                    if group != target_group:
                        other_ramp = group_config["target_concurrency"]
                        self.assertEqual(len(other_ramp), 1)
                        self.assertEqual(other_ramp[0].concurrency, 0)
                        self.assertEqual(other_ramp[0].duration_seconds, 1)
    
    def test_all_query_groups_power2_config(self):
        """Test power-of-2 config for each query group"""
        duration = 3600
        ramp_step = 10
        
        for target_group in QueryGroup:
            with self.subTest(target_group=target_group):
                config_dict = ProductionLoadConfig.get_single_group_power2_ramp_config(
                    target_group, ramp_step, duration
                )
                
                # Target group gets power-of-2 ramp
                target_config = config_dict[target_group]
                self.assertEqual(target_config["load_mode"], LoadMode.CONCURRENCY)
                target_ramp = target_config["target_concurrency"]
                self.assertGreater(len(target_ramp), 1)
                self.assertEqual(target_ramp[0].concurrency, 1)
                
                # All other groups get zero load
                for group, group_config in config_dict.items():
                    if group != target_group:
                        other_ramp = group_config["target_concurrency"]
                        self.assertEqual(len(other_ramp), 1)
                        self.assertEqual(other_ramp[0].concurrency, 0)
                        self.assertEqual(other_ramp[0].duration_seconds, 1)

if __name__ == '__main__':
    unittest.main()
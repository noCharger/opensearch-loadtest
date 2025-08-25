from typing import Dict
from ..loadtest.config import LoadMode
from ..utils.query_groups import QueryGroup
from ..utils.ramp_builder import RampBuilder

class ProductionLoadConfig:
    """Production load test configurations for 10TB dataset"""
    
    @staticmethod
    def get_conservative_ramp_config(ramp_step_minutes: int = 5) -> Dict[QueryGroup, dict]:
        """Conservative ramp-up based on real production usage ratios - 1 hour duration"""
        step_duration = ramp_step_minutes * 60
        
        return {
            # STATS/aggregation heavy (45.91% + 46.96% COUNT usage)
            QueryGroup.TERMS_AGGREGATION: {
                "load_mode": LoadMode.CONCURRENCY,
                "target_concurrency": RampBuilder.linear_concurrency_ramp(
                    start=2, end=15, steps=12, step_duration=step_duration
                )
            },
            # Basic search/text operations (moderate usage)
            QueryGroup.TEXT_QUERYING: {
                "load_mode": LoadMode.CONCURRENCY,
                "target_concurrency": RampBuilder.linear_concurrency_ramp(
                    start=1, end=8, steps=12, step_duration=step_duration
                )
            },
            # Range queries (11.10% WHERE usage)
            QueryGroup.RANGE_QUERIES: {
                "load_mode": LoadMode.CONCURRENCY,
                "target_concurrency": RampBuilder.linear_concurrency_ramp(
                    start=1, end=6, steps=12, step_duration=step_duration
                )
            },
            # Sorting (7.71% SORT usage)
            QueryGroup.SORTING: {
                "load_mode": LoadMode.CONCURRENCY,
                "target_concurrency": RampBuilder.linear_concurrency_ramp(
                    start=1, end=4, steps=12, step_duration=step_duration
                )
            },
            # Date histogram (time functions ~12% combined)
            QueryGroup.DATE_HISTOGRAM: {
                "load_mode": LoadMode.CONCURRENCY,
                "target_concurrency": RampBuilder.linear_concurrency_ramp(
                    start=1, end=5, steps=12, step_duration=step_duration
                )
            }
        }
    
    @staticmethod
    def get_moderate_ramp_config(ramp_step_minutes: int = 5) -> Dict[QueryGroup, dict]:
        """Moderate ramp-up matching production usage patterns - 1 hour duration"""
        step_duration = ramp_step_minutes * 60
        
        return {
            # Heavy aggregation workload (dominant usage pattern)
            QueryGroup.TERMS_AGGREGATION: {
                "load_mode": LoadMode.CONCURRENCY,
                "target_concurrency": RampBuilder.linear_concurrency_ramp(
                    start=4, end=25, steps=12, step_duration=step_duration
                )
            },
            # Text search and processing
            QueryGroup.TEXT_QUERYING: {
                "load_mode": LoadMode.CONCURRENCY,
                "target_concurrency": RampBuilder.linear_concurrency_ramp(
                    start=2, end=12, steps=12, step_duration=step_duration
                )
            },
            # Range and filtering operations
            QueryGroup.RANGE_QUERIES: {
                "load_mode": LoadMode.CONCURRENCY,
                "target_concurrency": RampBuilder.linear_concurrency_ramp(
                    start=2, end=10, steps=12, step_duration=step_duration
                )
            },
            # Time-based aggregations
            QueryGroup.DATE_HISTOGRAM: {
                "load_mode": LoadMode.CONCURRENCY,
                "target_concurrency": RampBuilder.linear_concurrency_ramp(
                    start=2, end=8, steps=12, step_duration=step_duration
                )
            },
            # Sorting operations (lower priority)
            QueryGroup.SORTING: {
                "load_mode": LoadMode.CONCURRENCY,
                "target_concurrency": RampBuilder.linear_concurrency_ramp(
                    start=1, end=6, steps=12, step_duration=step_duration
                )
            }
        }
    
    @staticmethod
    def apply_config_to_queries(queries, config_dict):
        """Apply production config to query list"""
        for query in queries:
            if query.query_group in config_dict:
                group_config = config_dict[query.query_group]
                query.load_mode = group_config["load_mode"]
                query.target_concurrency = group_config["target_concurrency"]
from typing import Dict
from ..loadtest.config import LoadMode
from ..utils.query_groups import QueryGroup
from ..utils.ramp_builder import RampBuilder

class ProductionLoadConfig:
    """Production load test configurations for 10TB dataset"""
    
    @staticmethod
    def get_conservative_ramp_config(ramp_step_minutes: int = 5) -> Dict[QueryGroup, dict]:
        """Conservative ramp-up using concurrency mode"""
        step_duration = ramp_step_minutes * 60
        
        return {
            # STATS/aggregation heavy (45.91% + 46.96% COUNT usage)
            QueryGroup.TERMS_AGGREGATION: {
                "load_mode": LoadMode.CONCURRENCY,
                "target_concurrency": RampBuilder.linear_concurrency_ramp(
                    start=1, end=3, steps=12, step_duration=step_duration
                )
            },
            # Basic search/text operations (moderate usage)
            QueryGroup.TEXT_QUERYING: {
                "load_mode": LoadMode.CONCURRENCY,
                "target_concurrency": RampBuilder.linear_concurrency_ramp(
                    start=1, end=2, steps=12, step_duration=step_duration
                )
            },
            # Range queries (11.10% WHERE usage)
            QueryGroup.RANGE_QUERIES: {
                "load_mode": LoadMode.CONCURRENCY,
                "target_concurrency": RampBuilder.linear_concurrency_ramp(
                    start=1, end=2, steps=12, step_duration=step_duration
                )
            },
            # Sorting (7.71% SORT usage)
            QueryGroup.SORTING: {
                "load_mode": LoadMode.CONCURRENCY,
                "target_concurrency": RampBuilder.linear_concurrency_ramp(
                    start=1, end=1, steps=12, step_duration=step_duration
                )
            },
            # Date histogram (time functions ~12% combined)
            QueryGroup.DATE_HISTOGRAM: {
                "load_mode": LoadMode.CONCURRENCY,
                "target_concurrency": RampBuilder.linear_concurrency_ramp(
                    start=1, end=2, steps=12, step_duration=step_duration
                )
            }
        }
    
    @staticmethod
    def get_moderate_ramp_config(ramp_step_minutes: int = 5) -> Dict[QueryGroup, dict]:
        """Moderate ramp-up using concurrency mode"""
        step_duration = ramp_step_minutes * 60
        
        return {
            # Heavy aggregation workload (dominant usage pattern)
            QueryGroup.TERMS_AGGREGATION: {
                "load_mode": LoadMode.CONCURRENCY,
                "target_concurrency": RampBuilder.linear_concurrency_ramp(
                    start=1, end=5, steps=12, step_duration=step_duration
                )
            },
            # Text search and processing
            QueryGroup.TEXT_QUERYING: {
                "load_mode": LoadMode.CONCURRENCY,
                "target_concurrency": RampBuilder.linear_concurrency_ramp(
                    start=1, end=3, steps=12, step_duration=step_duration
                )
            },
            # Range and filtering operations
            QueryGroup.RANGE_QUERIES: {
                "load_mode": LoadMode.CONCURRENCY,
                "target_concurrency": RampBuilder.linear_concurrency_ramp(
                    start=1, end=3, steps=12, step_duration=step_duration
                )
            },
            # Time-based aggregations
            QueryGroup.DATE_HISTOGRAM: {
                "load_mode": LoadMode.CONCURRENCY,
                "target_concurrency": RampBuilder.linear_concurrency_ramp(
                    start=1, end=2, steps=12, step_duration=step_duration
                )
            },
            # Sorting operations (lower priority)
            QueryGroup.SORTING: {
                "load_mode": LoadMode.CONCURRENCY,
                "target_concurrency": RampBuilder.linear_concurrency_ramp(
                    start=1, end=2, steps=12, step_duration=step_duration
                )
            }
        }
    
    @staticmethod
    def get_concurrent_ramp_config(ramp_step_minutes: int = 10) -> Dict[QueryGroup, dict]:
        """Concurrent mode: exponential ramp 2->16 over full duration"""
        step_duration = ramp_step_minutes * 60
        # Calculate steps based on total duration (e.g., 7200s ÷ 600s = 12 steps)
        # This will be calculated dynamically based on actual test duration
        steps = 12  # Default for 2-hour test with 10-minute steps
        
        # Total concurrency distributed across groups
        return {
            QueryGroup.TERMS_AGGREGATION: {
                "load_mode": LoadMode.CONCURRENCY,
                "target_concurrency": RampBuilder.exponential_concurrency_ramp(
                    start=2, end=16, steps=steps, step_duration=step_duration
                )
            },
            QueryGroup.TEXT_QUERYING: {
                "load_mode": LoadMode.CONCURRENCY,
                "target_concurrency": RampBuilder.exponential_concurrency_ramp(
                    start=2, end=8, steps=steps, step_duration=step_duration
                )
            },
            QueryGroup.RANGE_QUERIES: {
                "load_mode": LoadMode.CONCURRENCY,
                "target_concurrency": RampBuilder.exponential_concurrency_ramp(
                    start=2, end=4, steps=steps, step_duration=step_duration
                )
            },
            QueryGroup.SORTING: {
                "load_mode": LoadMode.CONCURRENCY,
                "target_concurrency": RampBuilder.exponential_concurrency_ramp(
                    start=2, end=4, steps=steps, step_duration=step_duration
                )
            },
            QueryGroup.DATE_HISTOGRAM: {
                "load_mode": LoadMode.CONCURRENCY,
                "target_concurrency": RampBuilder.exponential_concurrency_ramp(
                    start=2, end=8, steps=steps, step_duration=step_duration
                )
            }
        }
    
    @staticmethod
    def get_dsl_high_concurrency_config(ramp_step_minutes: int = 10) -> Dict[str, dict]:
        """High concurrency DSL config: ramp to 200 total concurrency in 1 hour"""
        step_duration = ramp_step_minutes * 60
        steps = 6  # 6 steps over 1 hour with 10-minute intervals
        
        # Distribute 200 total concurrency across 5 DSL queries
        return {
            "match-all": {
                "load_mode": LoadMode.CONCURRENCY,
                "target_concurrency": RampBuilder.linear_concurrency_ramp(
                    start=5, end=50, steps=steps, step_duration=step_duration
                )
            },
            "desc_sort_timestamp": {
                "load_mode": LoadMode.CONCURRENCY,
                "target_concurrency": RampBuilder.linear_concurrency_ramp(
                    start=4, end=40, steps=steps, step_duration=step_duration
                )
            },
            "term": {
                "load_mode": LoadMode.CONCURRENCY,
                "target_concurrency": RampBuilder.linear_concurrency_ramp(
                    start=4, end=40, steps=steps, step_duration=step_duration
                )
            },
            "composite-terms": {
                "load_mode": LoadMode.CONCURRENCY,
                "target_concurrency": RampBuilder.linear_concurrency_ramp(
                    start=3, end=35, steps=steps, step_duration=step_duration
                )
            },
            "range-numeric": {
                "load_mode": LoadMode.CONCURRENCY,
                "target_concurrency": RampBuilder.linear_concurrency_ramp(
                    start=4, end=35, steps=steps, step_duration=step_duration
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
                if "target_concurrency" in group_config:
                    query.target_concurrency = group_config["target_concurrency"]
                if "target_qps" in group_config:
                    query.target_qps = group_config["target_qps"]
    
    @staticmethod
    def apply_dsl_config_to_queries(queries, config_dict):
        """Apply DSL-specific config to query list by query name"""
        for query in queries:
            if query.name in config_dict:
                query_config = config_dict[query.name]
                query.load_mode = query_config["load_mode"]
                if "target_concurrency" in query_config:
                    query.target_concurrency = query_config["target_concurrency"]
                if "target_qps" in query_config:
                    query.target_qps = query_config["target_qps"]
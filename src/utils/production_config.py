from typing import Dict
from ..loadtest.config import LoadMode, ConcurrencyRamp
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
        """Concurrent mode: exponential ramp 2->32 over full duration for PPL queries"""
        step_duration = ramp_step_minutes * 60
        steps = 12  # Default for 2-hour test with 10-minute steps
        
        # PPL query groups with exponential scaling
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
                    start=1, end=4, steps=steps, step_duration=step_duration
                )
            },
            QueryGroup.SORTING: {
                "load_mode": LoadMode.CONCURRENCY,
                "target_concurrency": RampBuilder.exponential_concurrency_ramp(
                    start=1, end=2, steps=steps, step_duration=step_duration
                )
            },
            QueryGroup.DATE_HISTOGRAM: {
                "load_mode": LoadMode.CONCURRENCY,
                "target_concurrency": RampBuilder.exponential_concurrency_ramp(
                    start=1, end=2, steps=steps, step_duration=step_duration
                )
            }
        }
    
    @staticmethod
    def get_dsl_concurrent_config(ramp_step_minutes: int = 10) -> Dict[str, dict]:
        """DSL concurrent config: exponential ramp 5->50 over full duration"""
        step_duration = ramp_step_minutes * 60
        steps = 12  # Default for 2-hour test with 10-minute steps
        
        # Moderate exponential ramp: 5 queries × 10 concurrent each = 50 total
        return {
            "desc_sort_timestamp": {
                "load_mode": LoadMode.CONCURRENCY,
                "target_concurrency": RampBuilder.exponential_concurrency_ramp(
                    start=1, end=10, steps=steps, step_duration=step_duration
                )
            },
            "match-all": {
                "load_mode": LoadMode.CONCURRENCY,
                "target_concurrency": RampBuilder.exponential_concurrency_ramp(
                    start=1, end=10, steps=steps, step_duration=step_duration
                )
            },
            "term": {
                "load_mode": LoadMode.CONCURRENCY,
                "target_concurrency": RampBuilder.exponential_concurrency_ramp(
                    start=1, end=10, steps=steps, step_duration=step_duration
                )
            },
            "composite-terms": {
                "load_mode": LoadMode.CONCURRENCY,
                "target_concurrency": RampBuilder.exponential_concurrency_ramp(
                    start=1, end=10, steps=steps, step_duration=step_duration
                )
            },
            "range-numeric": {
                "load_mode": LoadMode.CONCURRENCY,
                "target_concurrency": RampBuilder.exponential_concurrency_ramp(
                    start=1, end=10, steps=steps, step_duration=step_duration
                )
            }
        }
    
    @staticmethod
    def get_ppl_high_concurrency_config(ramp_step_minutes: int = 5, duration_seconds: int = 3600) -> Dict[str, dict]:
        """PPL high concurrency config: linear ramp 5->50 over test duration (safer)"""
        step_duration = ramp_step_minutes * 60
        steps = max(8, duration_seconds // step_duration)  # At least 8 steps for gradual ramp
        
        # Linear ramp: 5 PPL queries × 10 concurrent each = 50 total (much safer)
        return {
            "composite_terms": {
                "load_mode": LoadMode.CONCURRENCY,
                "target_concurrency": RampBuilder.linear_concurrency_ramp(
                    start=1, end=10, steps=steps, step_duration=step_duration
                )
            },
            "desc_sort_timestamp": {
                "load_mode": LoadMode.CONCURRENCY,
                "target_concurrency": RampBuilder.linear_concurrency_ramp(
                    start=1, end=10, steps=steps, step_duration=step_duration
                )
            },
            "range": {
                "load_mode": LoadMode.CONCURRENCY,
                "target_concurrency": RampBuilder.linear_concurrency_ramp(
                    start=1, end=10, steps=steps, step_duration=step_duration
                )
            },
            "default": {
                "load_mode": LoadMode.CONCURRENCY,
                "target_concurrency": RampBuilder.linear_concurrency_ramp(
                    start=1, end=10, steps=steps, step_duration=step_duration
                )
            },
            "term": {
                "load_mode": LoadMode.CONCURRENCY,
                "target_concurrency": RampBuilder.linear_concurrency_ramp(
                    start=1, end=10, steps=steps, step_duration=step_duration
                )
            }
        }
    
    @staticmethod
    def get_dsl_high_concurrency_config(ramp_step_minutes: int = 5, duration_seconds: int = 3600) -> Dict[str, dict]:
        """High concurrency DSL config: linear ramp 5->50 over test duration (safer)"""
        step_duration = ramp_step_minutes * 60
        steps = max(8, duration_seconds // step_duration)  # At least 8 steps for gradual ramp
        
        # Linear ramp: 5 queries × 10 concurrent each = 50 total (much safer)
        return {
            "desc_sort_timestamp": {
                "load_mode": LoadMode.CONCURRENCY,
                "target_concurrency": RampBuilder.linear_concurrency_ramp(
                    start=1, end=10, steps=steps, step_duration=step_duration
                )
            },
            "match-all": {
                "load_mode": LoadMode.CONCURRENCY,
                "target_concurrency": RampBuilder.linear_concurrency_ramp(
                    start=1, end=10, steps=steps, step_duration=step_duration
                )
            },
            "term": {
                "load_mode": LoadMode.CONCURRENCY,
                "target_concurrency": RampBuilder.linear_concurrency_ramp(
                    start=1, end=10, steps=steps, step_duration=step_duration
                )
            },
            "composite-terms": {
                "load_mode": LoadMode.CONCURRENCY,
                "target_concurrency": RampBuilder.linear_concurrency_ramp(
                    start=1, end=10, steps=steps, step_duration=step_duration
                )
            },
            "range-numeric": {
                "load_mode": LoadMode.CONCURRENCY,
                "target_concurrency": RampBuilder.linear_concurrency_ramp(
                    start=1, end=10, steps=steps, step_duration=step_duration
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
    def apply_single_group_config_to_queries(queries, config_dict, target_group: QueryGroup):
        """Apply single group exponential config, others get zero load"""
        for query in queries:
            if query.query_group == target_group and target_group in config_dict:
                group_config = config_dict[target_group]
                query.load_mode = group_config["load_mode"]
                query.target_concurrency = group_config["target_concurrency"]
            else:
                # Other groups get zero load (excluded from test)
                query.load_mode = LoadMode.CONCURRENCY
                query.target_concurrency = [ConcurrencyRamp(concurrency=0, duration_seconds=1)]
    
    @staticmethod
    def apply_ppl_config_to_queries(queries, config_dict):
        """Apply PPL-specific config to query list by query name"""
        for query in queries:
            if query.name in config_dict:
                query_config = config_dict[query.name]
                query.load_mode = query_config["load_mode"]
                if "target_concurrency" in query_config:
                    query.target_concurrency = query_config["target_concurrency"]
                if "target_qps" in query_config:
                    query.target_qps = query_config["target_qps"]
            else:
                # Default minimal load for queries not in config
                query.load_mode = LoadMode.CONCURRENCY
                query.target_concurrency = [ConcurrencyRamp(concurrency=1, duration_seconds=600)]
    

    
    @staticmethod
    def get_single_group_exponential_config(target_group: QueryGroup, ramp_step_minutes: int = 10, duration_seconds: int = 3600) -> Dict[QueryGroup, dict]:
        """Single query group exponential ramp: 1->64 over test duration"""
        step_duration = ramp_step_minutes * 60
        steps = max(6, duration_seconds // step_duration)  # At least 6 steps for exponential growth
        
        config = {}
        for group in QueryGroup:
            if group == target_group:
                # Target group gets exponential ramp
                config[group] = {
                    "load_mode": LoadMode.CONCURRENCY,
                    "target_concurrency": RampBuilder.exponential_concurrency_ramp(
                        start=1, end=64, steps=steps, step_duration=step_duration
                    )
                }
            else:
                # Other groups get zero load (excluded from test)
                config[group] = {
                    "load_mode": LoadMode.CONCURRENCY,
                    "target_concurrency": [ConcurrencyRamp(concurrency=0, duration_seconds=1)]
                }
        return config
    
    @staticmethod
    def get_single_query_exponential_config(target_query: str, ramp_step_minutes: int = 10, duration_seconds: int = 3600) -> Dict[str, dict]:
        """Single query exponential ramp: 1->64 over test duration"""
        step_duration = ramp_step_minutes * 60
        steps = max(6, duration_seconds // step_duration)  # At least 6 steps for exponential growth
        
        return {
            target_query: {
                "load_mode": LoadMode.CONCURRENCY,
                "target_concurrency": RampBuilder.exponential_concurrency_ramp(
                    start=1, end=64, steps=steps, step_duration=step_duration
                )
            }
        }
    
    @staticmethod
    def get_single_group_power2_ramp_config(target_group: QueryGroup, ramp_step_minutes: int = 10, duration_seconds: int = 3600) -> Dict[QueryGroup, dict]:
        """Single query group power-of-2 then linear ramp: 1->2->4->8->16->32->64->100->150->200..."""
        step_duration = ramp_step_minutes * 60
        steps = duration_seconds // step_duration  # Use all available steps
        
        config = {}
        for group in QueryGroup:
            if group == target_group:
                # Target group gets power-of-2 then linear ramp
                config[group] = {
                    "load_mode": LoadMode.CONCURRENCY,
                    "target_concurrency": RampBuilder.power_of_2_concurrency_ramp(
                        steps=steps, step_duration=step_duration
                    )
                }
            else:
                # Other groups get zero load (excluded from test)
                config[group] = {
                    "load_mode": LoadMode.CONCURRENCY,
                    "target_concurrency": [ConcurrencyRamp(concurrency=0, duration_seconds=1)]
                }
        return config
    
    @staticmethod
    def get_single_query_power2_ramp_config(target_query: str, ramp_step_minutes: int = 10, duration_seconds: int = 3600) -> Dict[str, dict]:
        """Single query power-of-2 then linear ramp: 1->2->4->8->16->32->64->100->150->200..."""
        step_duration = ramp_step_minutes * 60
        steps = duration_seconds // step_duration  # Use all available steps
        
        # Use conservative ramp for date histogram and keyword terms queries
        if target_query in ['date_histogram_hourly_agg', 'keyword_terms']:
            ramp_function = RampBuilder.conservative_concurrency_ramp
        else:
            ramp_function = RampBuilder.power_of_2_concurrency_ramp
        
        return {
            target_query: {
                "load_mode": LoadMode.CONCURRENCY,
                "target_concurrency": ramp_function(
                    steps=steps, step_duration=step_duration
                )
            }
        }
    
    @staticmethod
    def apply_single_query_config_to_queries(queries, config_dict, target_query: str, duration_seconds: int):
        """Apply single query config, others get zero load"""
        for query in queries:
            if query.name == target_query and target_query in config_dict:
                query_config = config_dict[target_query]
                query.load_mode = query_config["load_mode"]
                query.target_concurrency = query_config["target_concurrency"]
            else:
                # All other queries get zero load (excluded from test)
                query.load_mode = LoadMode.CONCURRENCY
                query.target_concurrency = [ConcurrencyRamp(concurrency=0, duration_seconds=1)]
    
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
            else:
                # Default minimal load for queries not in config
                query.load_mode = LoadMode.CONCURRENCY
                query.target_concurrency = [ConcurrencyRamp(concurrency=1, duration_seconds=600)]
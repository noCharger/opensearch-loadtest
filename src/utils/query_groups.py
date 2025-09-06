from enum import Enum
from typing import Dict, Set

class QueryGroup(Enum):
    TEXT_QUERYING = "text_querying"
    SORTING = "sorting"
    DATE_HISTOGRAM = "date_histogram"
    RANGE_QUERIES = "range_queries"
    TERMS_AGGREGATION = "terms_aggregation"

class QueryGroupMapper:
    """Maps query names to their respective groups based on Big 5 categorization"""
    
    GROUP_MAPPINGS: Dict[str, QueryGroup] = {
        # Text Querying
        "default": QueryGroup.TEXT_QUERYING,
        "term": QueryGroup.TEXT_QUERYING,
        "keyword_in_range": QueryGroup.TEXT_QUERYING,
        "keyword-in-range": QueryGroup.TEXT_QUERYING,  # DSL
        "query_string_on_message": QueryGroup.TEXT_QUERYING,
        "query-string-on-message": QueryGroup.TEXT_QUERYING,  # DSL
        "query_string_on_message_filtered": QueryGroup.TEXT_QUERYING,
        "query-string-on-message-filtered": QueryGroup.TEXT_QUERYING,  # DSL
        "query_string_on_message_filtered_sorted_num": QueryGroup.TEXT_QUERYING,
        "query-string-on-message-filtered-sorted-num": QueryGroup.TEXT_QUERYING,  # DSL
        "scroll": QueryGroup.TEXT_QUERYING,
        "match-all": QueryGroup.TEXT_QUERYING,  # DSL
        
        # Sorting
        "desc_sort_timestamp": QueryGroup.SORTING,
        "desc_sort_with_after_timestamp": QueryGroup.SORTING,
        "desc_sort_with_after_timestamp": QueryGroup.SORTING,  # DSL
        "asc_sort_timestamp": QueryGroup.SORTING,
        "asc_sort_with_after_timestamp": QueryGroup.SORTING,
        "desc_sort_timestamp_can_match_shortcut": QueryGroup.SORTING,
        "desc_sort_timestamp_no_can_match_shortcut": QueryGroup.SORTING,
        "asc_sort_timestamp_can_match_shortcut": QueryGroup.SORTING,
        "asc_sort_timestamp_no_can_match_shortcut": QueryGroup.SORTING,
        "sort_keyword_can_match_shortcut": QueryGroup.SORTING,
        "sort_keyword_no_can_match_shortcut": QueryGroup.SORTING,
        "sort_numeric_desc": QueryGroup.SORTING,
        "sort_numeric_asc": QueryGroup.SORTING,
        "sort_numeric_desc_with_match": QueryGroup.SORTING,
        "sort_numeric_asc_with_match": QueryGroup.SORTING,
        "range_with_asc_sort": QueryGroup.SORTING,
        "range_with_desc_sort": QueryGroup.SORTING,
        
        # Date Histogram
        "date_histogram_hourly_agg": QueryGroup.DATE_HISTOGRAM,
        "date_histogram_minute_agg": QueryGroup.DATE_HISTOGRAM,
        "composite_date_histogram_daily": QueryGroup.DATE_HISTOGRAM,
        "composite-date_histogram-daily": QueryGroup.DATE_HISTOGRAM,  # DSL
        "range_auto_date_histo": QueryGroup.DATE_HISTOGRAM,
        "range-auto-date-histo": QueryGroup.DATE_HISTOGRAM,  # DSL
        "range_auto_date_histo_with_metrics": QueryGroup.DATE_HISTOGRAM,
        "range-auto-date-histo-with-metrics": QueryGroup.DATE_HISTOGRAM,  # DSL
        
        # Range Queries
        "range": QueryGroup.RANGE_QUERIES,
        "range_numeric": QueryGroup.RANGE_QUERIES,
        "range-numeric": QueryGroup.RANGE_QUERIES,  # DSL
        "range_field_conjunction_big_range_big_term_query": QueryGroup.RANGE_QUERIES,
        "range_field_disjunction_big_range_small_term_query": QueryGroup.RANGE_QUERIES,
        "range_field_conjunction_small_range_small_term_query": QueryGroup.RANGE_QUERIES,
        "range_field_conjunction_small_range_big_term_query": QueryGroup.RANGE_QUERIES,
        
        # Terms Aggregation
        "terms_significant_1": QueryGroup.TERMS_AGGREGATION,
        "terms-significant-1": QueryGroup.TERMS_AGGREGATION,  # DSL
        "terms_significant_2": QueryGroup.TERMS_AGGREGATION,
        "terms-significant-2": QueryGroup.TERMS_AGGREGATION,  # DSL
        "multi_terms_keyword": QueryGroup.TERMS_AGGREGATION,
        "multi_terms-keyword": QueryGroup.TERMS_AGGREGATION,  # DSL
        "composite_terms": QueryGroup.TERMS_AGGREGATION,
        "composite-terms": QueryGroup.TERMS_AGGREGATION,  # DSL
        "composite_terms_keyword": QueryGroup.TERMS_AGGREGATION,
        "composite_terms-keyword": QueryGroup.TERMS_AGGREGATION,  # DSL
        "keyword_terms": QueryGroup.TERMS_AGGREGATION,
        "keyword-terms": QueryGroup.TERMS_AGGREGATION,  # DSL
        "keyword_terms_low_cardinality": QueryGroup.TERMS_AGGREGATION,
        "keyword-terms-low-cardinality": QueryGroup.TERMS_AGGREGATION,  # DSL
    }
    
    @classmethod
    def get_group(cls, query_name: str) -> QueryGroup:
        """Get the group for a query name, default to TEXT_QUERYING if not found"""
        return cls.GROUP_MAPPINGS.get(query_name, QueryGroup.TEXT_QUERYING)
    
    @classmethod
    def get_queries_by_group(cls, group: QueryGroup) -> Set[str]:
        """Get all query names belonging to a specific group"""
        return {name for name, g in cls.GROUP_MAPPINGS.items() if g == group}
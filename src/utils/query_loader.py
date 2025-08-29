import os
from pathlib import Path
from typing import List, Dict
from ..loadtest.config import QueryConfig, QueryType
from .query_groups import QueryGroupMapper
from .dsl_query_loader import DSLQueryLoader

class QueryLoader:
    @staticmethod
    def load_queries_from_directory(queries_dir: str = "queries", index_pattern: str = "big5*") -> List[QueryConfig]:
        """Load all .ppl query files from the queries directory with configurable index pattern"""
        project_root = Path(__file__).parent.parent.parent
        queries_path = project_root / queries_dir
        
        if not queries_path.exists():
            raise FileNotFoundError(f"Queries directory not found: {queries_path}")
        
        queries = []
        for query_file in queries_path.glob("*.ppl"):
            query_content = query_file.read_text().strip()
            # Replace 'big5' with the configured index pattern (add backticks for patterns with special chars)
            if '*' in index_pattern or '-' in index_pattern:
                query_content = query_content.replace("source = big5", f"source = `{index_pattern}`")
            else:
                query_content = query_content.replace("source = big5", f"source = {index_pattern}")
            query_name = query_file.stem
            
            queries.append(QueryConfig(
                name=query_name,
                query_type=QueryType.PPL,
                query=query_content,
                query_group=QueryGroupMapper.get_group(query_name)
            ))
        
        return sorted(queries, key=lambda q: q.name)
    
    @staticmethod
    def load_specific_queries(query_names: List[str], queries_dir: str = "queries", index_pattern: str = "big5*") -> List[QueryConfig]:
        """Load specific query files by name with configurable index pattern"""
        project_root = Path(__file__).parent.parent.parent
        queries_path = project_root / queries_dir
        
        queries = []
        for query_name in query_names:
            query_file = queries_path / f"{query_name}.ppl"
            if not query_file.exists():
                raise FileNotFoundError(f"Query file not found: {query_file}")
            
            query_content = query_file.read_text().strip()
            # Replace 'big5' with the configured index pattern (add backticks for patterns with special chars)
            if '*' in index_pattern or '-' in index_pattern:
                query_content = query_content.replace("source = big5", f"source = `{index_pattern}`")
            else:
                query_content = query_content.replace("source = big5", f"source = {index_pattern}")
            queries.append(QueryConfig(
                name=query_name,
                query_type=QueryType.PPL,
                query=query_content,
                query_group=QueryGroupMapper.get_group(query_name)
            ))
        
        return queries
    
    @staticmethod
    def load_mixed_queries(ppl_queries_dir: str = "queries", dsl_json_file: str = None, index_pattern: str = "big5*") -> List[QueryConfig]:
        """Load both PPL and DSL queries for mixed load testing"""
        queries = []
        
        # Load PPL queries
        try:
            ppl_queries = QueryLoader.load_queries_from_directory(ppl_queries_dir, index_pattern)
            queries.extend(ppl_queries)
            print(f"Loaded {len(ppl_queries)} PPL queries")
        except FileNotFoundError:
            print(f"PPL queries directory not found: {ppl_queries_dir}")
        
        # Load DSL queries if JSON file provided
        if dsl_json_file:
            try:
                dsl_queries = DSLQueryLoader.load_queries_from_json(dsl_json_file, index_pattern)
                queries.extend(dsl_queries)
                print(f"Loaded {len(dsl_queries)} DSL queries")
            except FileNotFoundError:
                print(f"DSL queries file not found: {dsl_json_file}")
        
        return sorted(queries, key=lambda q: q.name)
    
    @staticmethod
    def load_dsl_queries(json_file: str, index_pattern: str = "big5*", selected_queries: List[str] = None) -> List[QueryConfig]:
        """Load DSL queries from JSON file"""
        return DSLQueryLoader.load_queries_from_json(json_file, index_pattern, selected_queries)
    
    @staticmethod
    def load_one_query_per_group(queries_dir: str = "queries", index_pattern: str = "big5*") -> List[QueryConfig]:
        """Load only one query from each query group for minimal testing"""
        all_queries = QueryLoader.load_queries_from_directory(queries_dir, index_pattern)
        
        # Group queries by their query group
        from collections import defaultdict
        grouped_queries = defaultdict(list)
        for query in all_queries:
            if query.query_group:
                grouped_queries[query.query_group].append(query)
        
        # Take first query from each group
        selected_queries = []
        for group, queries in grouped_queries.items():
            if queries:
                selected_queries.append(queries[0])  # Take first query from group
                print(f"Selected {queries[0].name} for group {group.value}")
        
        return selected_queries
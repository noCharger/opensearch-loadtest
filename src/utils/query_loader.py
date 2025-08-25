import os
from pathlib import Path
from typing import List, Dict
from ..loadtest.config import QueryConfig, QueryType
from .query_groups import QueryGroupMapper

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
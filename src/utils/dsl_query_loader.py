import json
from pathlib import Path
from typing import List, Dict, Any
from ..loadtest.config import QueryConfig, QueryType
from .query_groups import QueryGroupMapper

class DSLQueryLoader:
    @staticmethod
    def load_queries_from_json(json_file: str, index_pattern: str = "big5*", selected_queries: List[str] = None) -> List[QueryConfig]:
        """Load DSL queries from JSON file with Rally-style format"""
        project_root = Path(__file__).parent.parent.parent
        json_path = project_root / json_file
        
        if not json_path.exists():
            raise FileNotFoundError(f"DSL queries file not found: {json_path}")
        
        with open(json_path, 'r') as f:
            data = json.load(f)
        
        queries = []
        
        # Handle both array format and object with operations key
        operations = data if isinstance(data, list) else data.get('operations', [])
        
        # Default selected queries for high concurrency test
        if selected_queries is None:
            selected_queries = ['match-all', 'desc_sort_timestamp', 'term', 'composite-terms', 'range-numeric']
        
        for operation in operations:
            if operation.get('operation-type') == 'search':
                query_name = operation.get('name', f"dsl_query_{len(queries)}")
                
                # Only load selected queries
                if selected_queries and query_name not in selected_queries:
                    continue
                
                # Extract query body
                body = operation.get('body', {})
                
                # Replace index pattern in operation
                index = operation.get('index', index_pattern)
                if '{{index_name' in index:
                    # Handle template variables like {{index_name | default('big5')}}
                    index = index_pattern
                
                queries.append(QueryConfig(
                    name=query_name,
                    query_type=QueryType.DSL,
                    query=json.dumps(body),
                    index=index_pattern,  # Use the passed index_pattern instead of hardcoded index
                    query_group=QueryGroupMapper.get_group(query_name)
                ))
        
        return sorted(queries, key=lambda q: q.name)
    
    @staticmethod
    def load_specific_dsl_queries(query_names: List[str], json_file: str, index_pattern: str = "big5*") -> List[QueryConfig]:
        """Load specific DSL queries by name from JSON file"""
        all_queries = DSLQueryLoader.load_queries_from_json(json_file, index_pattern)
        
        # Filter by requested names
        selected_queries = [q for q in all_queries if q.name in query_names]
        
        # Check for missing queries
        found_names = {q.name for q in selected_queries}
        missing_names = set(query_names) - found_names
        if missing_names:
            raise ValueError(f"DSL queries not found: {missing_names}")
        
        return selected_queries
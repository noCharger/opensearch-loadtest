#!/usr/bin/env python3

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from src.utils.query_loader import QueryLoader
from src.loadtest.config import QueryType

def test_dsl_loader():
    """Test DSL query loading functionality"""
    print("Testing DSL Query Loader...")
    
    try:
        # Test loading DSL queries
        queries = QueryLoader.load_dsl_queries("queries/dsl_queries.json", "big5*")
        
        print(f"Loaded {len(queries)} DSL queries:")
        for query in queries:
            print(f"  - {query.name} (Type: {query.query_type.value}, Index: {query.index})")
            print(f"    Query preview: {query.query[:100]}...")
            print()
        
        # Test mixed query loading
        print("\nTesting mixed query loading...")
        mixed_queries = QueryLoader.load_mixed_queries(
            ppl_queries_dir="queries",
            dsl_json_file="queries/dsl_queries.json",
            index_pattern="big5*"
        )
        
        ppl_count = sum(1 for q in mixed_queries if q.query_type == QueryType.PPL)
        dsl_count = sum(1 for q in mixed_queries if q.query_type == QueryType.DSL)
        
        print(f"Mixed queries loaded: {len(mixed_queries)} total")
        print(f"  - PPL queries: {ppl_count}")
        print(f"  - DSL queries: {dsl_count}")
        
        print("\nDSL Query Loader test completed successfully!")
        
    except Exception as e:
        print(f"Test failed: {e}")
        return False
    
    return True

if __name__ == "__main__":
    success = test_dsl_loader()
    sys.exit(0 if success else 1)
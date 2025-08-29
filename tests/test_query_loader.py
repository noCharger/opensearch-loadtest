#!/usr/bin/env python3
import unittest
import sys
import os
import tempfile
from unittest.mock import patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from src.utils.query_loader import QueryLoader

class TestQueryLoader(unittest.TestCase):
    
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        
    def tearDown(self):
        import shutil
        shutil.rmtree(self.temp_dir)
        
    def test_load_queries_from_directory(self):
        # Create test query files
        query1_path = os.path.join(self.temp_dir, 'test1.ppl')
        query2_path = os.path.join(self.temp_dir, 'test2.ppl')
        
        with open(query1_path, 'w') as f:
            f.write('source=test | head 10')
        with open(query2_path, 'w') as f:
            f.write('source=test | stats count()')
            
        loader = QueryLoader()
        queries = loader.load_queries_from_directory(self.temp_dir)
        
        self.assertEqual(len(queries), 2)
        self.assertIn('test1', [q.name for q in queries])
        self.assertIn('test2', [q.name for q in queries])
        
    def test_load_specific_queries(self):
        # Create test query files
        query1_path = os.path.join(self.temp_dir, 'test1.ppl')
        query2_path = os.path.join(self.temp_dir, 'test2.ppl')
        
        with open(query1_path, 'w') as f:
            f.write('source=test | head 10')
        with open(query2_path, 'w') as f:
            f.write('source=test | stats count()')
            
        loader = QueryLoader()
        queries = loader.load_specific_queries(self.temp_dir, ['test1'])
        
        self.assertEqual(len(queries), 1)
        self.assertEqual(queries[0].name, 'test1')
        
    def test_empty_directory(self):
        loader = QueryLoader()
        queries = loader.load_queries_from_directory(self.temp_dir)
        
        self.assertEqual(len(queries), 0)
        
    def test_non_ppl_files_ignored(self):
        # Create non-PPL files
        txt_path = os.path.join(self.temp_dir, 'readme.txt')
        ppl_path = os.path.join(self.temp_dir, 'test.ppl')
        
        with open(txt_path, 'w') as f:
            f.write('This is not a query')
        with open(ppl_path, 'w') as f:
            f.write('source=test | head 10')
            
        loader = QueryLoader()
        queries = loader.load_queries_from_directory(self.temp_dir)
        
        self.assertEqual(len(queries), 1)
        self.assertEqual(queries[0].name, 'test')

if __name__ == '__main__':
    unittest.main()
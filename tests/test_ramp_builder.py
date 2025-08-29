#!/usr/bin/env python3
import unittest
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from src.utils.ramp_builder import RampBuilder

class TestRampBuilder(unittest.TestCase):
    
    def test_linear_concurrency_ramp(self):
        ramp = RampBuilder.linear_concurrency_ramp(start=1, end=5, steps=5, step_duration=60)
        
        self.assertEqual(len(ramp), 5)
        self.assertEqual(ramp[0]['concurrency'], 1)
        self.assertEqual(ramp[-1]['concurrency'], 5)
        self.assertEqual(ramp[0]['duration'], 60)
        
    def test_exponential_concurrency_ramp(self):
        ramp = RampBuilder.exponential_concurrency_ramp(start=1, end=8, steps=4, step_duration=30)
        
        self.assertEqual(len(ramp), 4)
        self.assertEqual(ramp[0]['concurrency'], 1)
        self.assertEqual(ramp[-1]['concurrency'], 8)
        # Exponential growth should have increasing steps
        self.assertLess(ramp[1]['concurrency'] - ramp[0]['concurrency'], 
                       ramp[-1]['concurrency'] - ramp[-2]['concurrency'])
        
    def test_qps_ramp(self):
        ramp = RampBuilder.qps_ramp(start_qps=10, end_qps=100, steps=3, step_duration=120)
        
        self.assertEqual(len(ramp), 3)
        self.assertEqual(ramp[0]['qps'], 10)
        self.assertEqual(ramp[-1]['qps'], 100)
        self.assertEqual(ramp[0]['duration'], 120)
        
    def test_single_step_ramp(self):
        ramp = RampBuilder.linear_concurrency_ramp(start=5, end=5, steps=1, step_duration=300)
        
        self.assertEqual(len(ramp), 1)
        self.assertEqual(ramp[0]['concurrency'], 5)
        self.assertEqual(ramp[0]['duration'], 300)
        
    def test_zero_steps_raises_error(self):
        with self.assertRaises(ValueError):
            RampBuilder.linear_concurrency_ramp(start=1, end=5, steps=0, step_duration=60)

if __name__ == '__main__':
    unittest.main()
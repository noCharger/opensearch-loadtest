from typing import List, Union
from ..loadtest.config import ConcurrencyRamp, QPSRamp

class RampBuilder:
    @staticmethod
    def linear_concurrency_ramp(start: int, end: int, steps: int, step_duration: int) -> List[ConcurrencyRamp]:
        """Build linear concurrency ramp from start to end"""
        if steps <= 1:
            return [ConcurrencyRamp(concurrency=end, duration_seconds=step_duration)]
        
        ramps = []
        step_size = (end - start) / (steps - 1)
        
        for i in range(steps):
            concurrency = int(start + i * step_size)
            ramps.append(ConcurrencyRamp(concurrency=concurrency, duration_seconds=step_duration))
        
        return ramps
    
    @staticmethod
    def linear_qps_ramp(start: float, end: float, steps: int, step_duration: int) -> List[QPSRamp]:
        """Build linear QPS ramp from start to end"""
        if steps <= 1:
            return [QPSRamp(qps=end, duration_seconds=step_duration)]
        
        ramps = []
        step_size = (end - start) / (steps - 1)
        
        for i in range(steps):
            qps = start + i * step_size
            ramps.append(QPSRamp(qps=qps, duration_seconds=step_duration))
        
        return ramps
    
    @staticmethod
    def exponential_concurrency_ramp(start: int, end: int, steps: int, step_duration: int) -> List[ConcurrencyRamp]:
        """Build exponential concurrency ramp from start to end"""
        if steps <= 1:
            return [ConcurrencyRamp(concurrency=end, duration_seconds=step_duration)]
        
        ramps = []
        multiplier = (end / start) ** (1 / (steps - 1))
        
        for i in range(steps):
            concurrency = int(start * (multiplier ** i))
            ramps.append(ConcurrencyRamp(concurrency=concurrency, duration_seconds=step_duration))
        
        return ramps
    
    @staticmethod
    def power_of_2_concurrency_ramp(steps: int, step_duration: int) -> List[ConcurrencyRamp]:
        """Build power of 2 then linear ramp: 1->2->4->8->16->32->64->100->150->200..."""
        if steps <= 1:
            return [ConcurrencyRamp(concurrency=1, duration_seconds=step_duration)]
        
        ramps = []
        for i in range(steps):
            if i < 7:  # First 7 steps: power of 2 (1, 2, 4, 8, 16, 32, 64)
                concurrency = 2 ** i
            else:  # After step 7: linear increments of 50 starting from 100
                concurrency = 100 + (i - 7) * 50  # 100, 150, 200, 250...
            ramps.append(ConcurrencyRamp(concurrency=concurrency, duration_seconds=step_duration))
        
        return ramps
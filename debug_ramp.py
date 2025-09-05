#!/usr/bin/env python3

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from src.utils.ramp_builder import RampBuilder

# Test exponential ramp with 12 steps (1 hour / 5 minutes)
print("Testing exponential ramp: start=1, end=40, steps=12, step_duration=300")
ramps = RampBuilder.exponential_concurrency_ramp(start=1, end=40, steps=12, step_duration=300)

for i, ramp in enumerate(ramps):
    print(f"Step {i+1}: {ramp.concurrency} concurrent for {ramp.duration_seconds}s")

print(f"\nTotal steps: {len(ramps)}")
print(f"Final concurrency: {ramps[-1].concurrency}")
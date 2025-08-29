#!/usr/bin/env python3
"""
Simple test runner for OpenSearch PPL Load Test Framework
"""
import subprocess
import sys
import os

def run_tests():
    """Run all tests and return success status"""
    print("Running OpenSearch PPL Load Test Framework Tests...")
    print("=" * 60)
    
    # Run tests in the tests directory
    test_commands = [
        ['python', '-m', 'pytest', 'tests/', '-v'],  # If pytest is available
        ['python', 'tests/run_all_tests.py'],        # Fallback to unittest
        ['python', 'test_analyze_logs.py']           # Root level test
    ]
    
    success = True
    
    for cmd in test_commands:
        try:
            print(f"\nRunning: {' '.join(cmd)}")
            result = subprocess.run(cmd, capture_output=True, text=True)
            
            if result.returncode == 0:
                print("✓ PASSED")
                if result.stdout:
                    print(result.stdout)
            else:
                print("✗ FAILED")
                if result.stderr:
                    print(result.stderr)
                success = False
                
        except FileNotFoundError:
            if 'pytest' in cmd:
                print("pytest not found, trying unittest...")
                continue
            else:
                print(f"Command not found: {' '.join(cmd)}")
                success = False
        except Exception as e:
            print(f"Error running {' '.join(cmd)}: {e}")
            success = False
    
    print("\n" + "=" * 60)
    if success:
        print("✓ All tests passed!")
        return True
    else:
        print("✗ Some tests failed!")
        return False

if __name__ == '__main__':
    success = run_tests()
    sys.exit(0 if success else 1)
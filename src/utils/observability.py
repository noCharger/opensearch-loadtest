import time
import threading
from collections import defaultdict
from .query_groups import QueryGroup

class ObservabilityMonitor:
    def __init__(self):
        self.start_time = time.time()
        self.query_counters = defaultdict(lambda: {"success": 0, "error": 0})
        self.concurrent_requests = defaultdict(int)
        self.group_concurrent_requests = defaultdict(int)
        self.lock = threading.Lock()
        self.running = False
        self._load_paused = False
    
    def start_monitoring(self):
        self.running = True
        self.monitor_thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self.monitor_thread.start()
    
    def stop_monitoring(self):
        self.running = False
    
    def record_request(self, query_name: str, success: bool):
        with self.lock:
            if success:
                self.query_counters[query_name]["success"] += 1
            else:
                self.query_counters[query_name]["error"] += 1
    
    def start_request(self, query_name: str, query_group: QueryGroup = None):
        with self.lock:
            self.concurrent_requests[query_name] += 1
            if query_group:
                self.group_concurrent_requests[query_group] += 1
    
    def end_request(self, query_name: str, query_group: QueryGroup = None):
        with self.lock:
            self.concurrent_requests[query_name] = max(0, self.concurrent_requests[query_name] - 1)
            if query_group:
                self.group_concurrent_requests[query_group] = max(0, self.group_concurrent_requests[query_group] - 1)
    
    def _monitor_loop(self):
        while self.running:
            time.sleep(1)
            elapsed = time.time() - self.start_time
            
            with self.lock:
                total_success = sum(c["success"] for c in self.query_counters.values())
                total_error = sum(c["error"] for c in self.query_counters.values())
                total_requests = total_success + total_error
                
                if total_requests > 0:
                    current_rps = total_requests / elapsed
                    success_rate = (total_success / total_requests) * 100
                    total_concurrent = sum(self.concurrent_requests.values())
                    
                    # Group concurrency summary
                    group_summary = " | ".join([f"{g.value}: {c}" for g, c in self.group_concurrent_requests.items() if c > 0])
                    
                    status = " [PAUSED]" if hasattr(self, '_load_paused') and self._load_paused else ""
                    print(f"[{elapsed:.0f}s] Requests: {total_requests} | Success: {success_rate:.1f}% | RPS: {current_rps:.1f} | Concurrent: {total_concurrent}{status}")
                    if group_summary:
                        print(f"[{elapsed:.0f}s] Group Concurrency: {group_summary}")
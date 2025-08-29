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
        self.max_concurrency = 0
        self.per_query_max_concurrency = defaultdict(int)  # Track max concurrency per query
        self.lock = threading.Lock()
        self.running = False
    
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
            # Update max concurrency with current total
            total_concurrent = sum(self.concurrent_requests.values())
            self.max_concurrency = max(self.max_concurrency, total_concurrent)
            # Update per-query max concurrency
            current_query_concurrency = self.concurrent_requests[query_name]
            self.per_query_max_concurrency[query_name] = max(
                self.per_query_max_concurrency[query_name], 
                current_query_concurrency
            )
    
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
                    
                    avg_latency_ms = (elapsed / total_requests) * 1000 if total_requests > 0 else 0
                    print(f"[{elapsed:.0f}s] Requests: {total_requests} | Success: {success_rate:.1f}% | RPS: {current_rps:.1f} | Concurrent: {total_concurrent} | Max: {self.max_concurrency} | Avg Latency: {avg_latency_ms:.1f}ms")
                    if group_summary:
                        print(f"[{elapsed:.0f}s] Group Concurrency: {group_summary}")
    
    def get_max_concurrency(self):
        """Get current max concurrency value"""
        with self.lock:
            # Also update with current total to ensure accuracy
            current_total = sum(self.concurrent_requests.values())
            self.max_concurrency = max(self.max_concurrency, current_total)
            return self.max_concurrency
    
    def get_query_max_concurrency(self, query_name: str):
        """Get max concurrency for a specific query"""
        with self.lock:
            return self.per_query_max_concurrency.get(query_name, 0)
    
    def get_group_stats(self, group):
        """Get statistics for a query group"""
        with self.lock:
            total_success = sum(self.query_counters[q]["success"] for q in self.query_counters if group.value in q)
            total_error = sum(self.query_counters[q]["error"] for q in self.query_counters if group.value in q)
            total_requests = total_success + total_error
            
            if total_requests == 0:
                return None
            
            return {
                'total_requests': total_requests,
                'success_requests': total_success,
                'error_requests': total_error,
                'success_rate': (total_success / total_requests) * 100
            }
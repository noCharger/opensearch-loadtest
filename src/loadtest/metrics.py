import time
from threading import Lock
from dataclasses import dataclass, field
from typing import List, Dict
from collections import defaultdict

@dataclass
class RequestMetric:
    timestamp: float
    duration: float
    success: bool
    query_name: str
    error: str = None

@dataclass
class MetricsCollector:
    _metrics: List[RequestMetric] = field(default_factory=list)
    _lock: Lock = field(default_factory=Lock)
    
    def record(self, duration: float, success: bool, query_name: str, error: str = None):
        with self._lock:
            self._metrics.append(RequestMetric(
                timestamp=time.time(),
                duration=duration,
                success=success,
                query_name=query_name,
                error=error
            ))
    
    def get_summary(self) -> Dict[str, dict]:
        with self._lock:
            if not self._metrics:
                return {"overall": {"total": 0, "success": 0, "errors": 0, "avg_duration": 0}}
            
            # Overall stats
            total = len(self._metrics)
            success = sum(1 for m in self._metrics if m.success)
            errors = total - success
            avg_duration = sum(m.duration for m in self._metrics) / total
            time_span = max(m.timestamp for m in self._metrics) - min(m.timestamp for m in self._metrics)
            
            results = {
                "overall": {
                    "total": total,
                    "success": success,
                    "errors": errors,
                    "success_rate": success / total * 100,
                    "avg_duration": avg_duration,
                    "rps": total / time_span if time_span > 0 else 0
                }
            }
            
            # Per-query stats
            query_metrics = defaultdict(list)
            for m in self._metrics:
                query_metrics[m.query_name].append(m)
            
            for query_name, metrics in query_metrics.items():
                q_total = len(metrics)
                q_success = sum(1 for m in metrics if m.success)
                q_errors = q_total - q_success
                
                durations = [m.duration for m in metrics]
                q_avg_duration = sum(durations) / q_total
                q_min_duration = min(durations)
                q_max_duration = max(durations)
                q_p90_duration = sorted(durations)[int(0.9 * len(durations))] if durations else 0
                
                q_time_span = max(m.timestamp for m in metrics) - min(m.timestamp for m in metrics)
                q_rps = q_total / q_time_span if q_time_span > 0 else 0
                
                # Calculate throughput stats (RPS over 5-second windows)
                throughput_windows = self._calculate_throughput_windows(metrics)
                
                results[query_name] = {
                    "total": q_total,
                    "success": q_success,
                    "errors": q_errors,
                    "success_rate": q_success / q_total * 100,
                    "avg_duration": q_avg_duration,
                    "min_duration": q_min_duration,
                    "max_duration": q_max_duration,
                    "p90_duration": q_p90_duration,
                    "rps": q_rps,
                    "avg_throughput": sum(throughput_windows) / len(throughput_windows) if throughput_windows else 0,
                    "min_throughput": min(throughput_windows) if throughput_windows else 0,
                    "max_throughput": max(throughput_windows) if throughput_windows else 0
                }
            
            return results
    
    def _calculate_throughput_windows(self, metrics: List[RequestMetric]) -> List[float]:
        """Calculate throughput in 5-second windows"""
        if not metrics:
            return []
        
        start_time = min(m.timestamp for m in metrics)
        end_time = max(m.timestamp for m in metrics)
        window_size = 5.0
        
        throughputs = []
        current_time = start_time
        
        while current_time < end_time:
            window_end = current_time + window_size
            window_requests = [m for m in metrics if current_time <= m.timestamp < window_end]
            throughput = len(window_requests) / window_size if window_requests else 0
            throughputs.append(throughput)
            current_time = window_end
        
        return throughputs
import time
import threading
import json
import uuid
import signal
import sys
from concurrent.futures import ThreadPoolExecutor
from opensearchpy import OpenSearch
from typing import List
from .config import LoadTestConfig, QueryConfig, QueryType, LoadMode
from .metrics import MetricsCollector
from ..utils.wal_logger import WALLogger
from ..utils.observability import ObservabilityMonitor
from ..utils.metrics_exporter import MetricsExporter
from ..utils.cpu_monitor import CPUMonitor

class LoadTester:
    def __init__(self, config: LoadTestConfig):
        self.config = config
        self.client = OpenSearch(**config.to_client_config())
        self.metrics = MetricsCollector()
        self._stop_event = threading.Event()
        self.execution_id = str(uuid.uuid4())[:8]
        self.wal_logger = WALLogger(self.execution_id)
        self.monitor = ObservabilityMonitor()
        self._shutdown_initiated = False
        self._load_paused = False
        self.cpu_monitor = CPUMonitor(threshold=90.0, check_interval=5)
        
        # Create separate client for metrics export
        metrics_client = OpenSearch(**config.to_metrics_client_config())
        self.metrics_exporter = MetricsExporter(self.client, metrics_client, self.execution_id)
        
        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
        # Setup CPU monitor callbacks
        self.cpu_monitor.set_callbacks(self._pause_load, self._resume_load)
    
    def print_execution_plan(self):
        """Print the execution plan before running tests"""
        print("\n=== Load Test Execution Plan ===")
        print(f"Duration: {self.config.duration_seconds} seconds")
        print(f"Target: {self.config.host}:{self.config.port}")
        print(f"Total queries: {len(self.config.queries)}")
        
        total_qps = sum(
            q.target_qps if q.load_mode == LoadMode.QPS and isinstance(q.target_qps, (int, float))
            else q.target_qps[0].qps if q.load_mode == LoadMode.QPS and q.target_qps
            else 0 for q in self.config.queries
        )
        print(f"Total target QPS: {total_qps}")
        
        print("\nQuery Details:")
        for i, query in enumerate(self.config.queries, 1):
            print(f"  {i}. {query.name}")
            print(f"     Type: {query.query_type.value.upper()}")
            if query.load_mode == LoadMode.QPS:
                if isinstance(query.target_qps, (int, float)):
                    print(f"     Target QPS: {query.target_qps}")
                else:
                    print(f"     QPS Ramp: {[(r.qps, r.duration_seconds) for r in query.target_qps]}")
            else:
                if isinstance(query.target_concurrency, int):
                    print(f"     Target Concurrency: {query.target_concurrency}")
                else:
                    print(f"     Concurrency Ramp: {[(r.concurrency, r.duration_seconds) for r in query.target_concurrency]}")
            if query.index:
                print(f"     Index: {query.index}")
            print(f"     Query: {query.query[:100]}{'...' if len(query.query) > 100 else ''}")
        print("\n" + "="*50)
        self._print_qps_timeline()
    
    def _pause_load(self):
        """Pause load generation due to high CPU"""
        self._load_paused = True
        self.monitor._load_paused = True
        self.wal_logger.log("EXECUTION", "CPU_PAUSE")
    
    def _resume_load(self):
        """Resume load generation after CPU drops"""
        self._load_paused = False
        self.monitor._load_paused = False
        self.wal_logger.log("EXECUTION", "CPU_RESUME")
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully"""
        if not self._shutdown_initiated:
            self._shutdown_initiated = True
            print(f"\n\nReceived signal {signum}. Initiating graceful shutdown...")
            self._stop_event.set()
            self.monitor.stop_monitoring()
            self.cpu_monitor.stop_monitoring()
            self.wal_logger.log("EXECUTION", "INTERRUPTED")
    
    def run_test(self):
        """Execute load test with configured queries"""
        self.print_execution_plan()
        
        print(f"\nStarting load test... [Execution ID: {self.execution_id}]")
        print("CPU monitoring enabled - will pause if CPU > 90%")
        self.wal_logger.log("EXECUTION", "START")
        self.monitor.start_monitoring()
        self.cpu_monitor.start_monitoring()
        self.metrics_exporter.export_node_stats()
        self.test_start_time = time.time()
        
        # Calculate max workers based on concurrency targets
        max_concurrent = sum(self._get_max_concurrency(q) for q in self.config.queries)
        max_workers = max(50, max_concurrent + 20)  # Buffer for QPS queries
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit worker threads for each query
            futures = []
            for query_config in self.config.queries:
                # Calculate workers based on load mode
                if query_config.load_mode == LoadMode.CONCURRENCY:
                    workers_needed = self._get_max_concurrency(query_config)
                else:
                    max_qps = self._get_max_qps(query_config)
                    if max_qps < 1.0:
                        workers_needed = 1
                    else:
                        workers_needed = max(1, min(int(max_qps), 5))
                
                # Start multiple workers for higher QPS
                for worker_id in range(workers_needed):
                    future = executor.submit(self._query_worker, query_config, worker_id, workers_needed)
                    futures.append(future)
            
            # Wait for test duration with periodic metrics collection
            start_time = time.time()
            last_metrics_time = start_time
            
            while time.time() - start_time < self.config.duration_seconds:
                current_time = time.time()
                
                # Export node stats every 30 seconds
                if current_time - last_metrics_time >= 30:
                    self.metrics_exporter.export_node_stats()
                    last_metrics_time = current_time
                
                time.sleep(min(1, self.config.duration_seconds - (current_time - start_time)))
            
            if not self._shutdown_initiated:
                self._stop_event.set()
                self.monitor.stop_monitoring()
                self.cpu_monitor.stop_monitoring()
            
            # Final metrics export
            try:
                self.metrics_exporter.export_node_stats()
            except Exception as e:
                print(f"Warning: Failed to export final metrics: {e}")
            
            # Wait for all workers to complete with timeout
            print("Waiting for active requests to complete...")
            for i, future in enumerate(futures):
                try:
                    future.result(timeout=30)  # 30 second timeout per worker
                except Exception as e:
                    print(f"Worker {i} did not complete cleanly: {e}")
        
        results = self.metrics.get_summary()
        status = "INTERRUPTED" if self._shutdown_initiated else "END"
        self.wal_logger.log("EXECUTION", status)
        
        if self._shutdown_initiated:
            print("\nGraceful shutdown completed. Partial results:")
        
        return results
    
    def _query_worker(self, query_config: QueryConfig, worker_id: int, total_workers: int):
        """Worker thread for a specific query"""
        self.wal_logger.log(query_config.name, "WORKER_START")
        
        if query_config.load_mode == LoadMode.CONCURRENCY:
            self._concurrency_worker(query_config)
        else:
            self._qps_worker(query_config, total_workers)
        
        self.wal_logger.log(query_config.name, "WORKER_END")
    
    def _qps_worker(self, query_config: QueryConfig, total_workers: int):
        """QPS-based worker"""
        next_request_time = time.time()
        
        while not self._stop_event.is_set():
            # Check if load is paused due to high CPU
            if self._load_paused:
                time.sleep(1)
                continue
                
            current_qps = self._get_current_qps(query_config)
            if current_qps <= 0:
                time.sleep(0.1)
                continue
            
            worker_qps = current_qps / total_workers
            interval = 1.0 / worker_qps if worker_qps > 0 else 100.0
            
            current_time = time.time()
            if current_time < next_request_time:
                time.sleep(next_request_time - current_time)
            
            next_request_time = time.time() + interval
            
            threading.Thread(
                target=self._execute_query_async,
                args=(query_config,),
                daemon=True
            ).start()
    
    def _concurrency_worker(self, query_config: QueryConfig):
        """Concurrency-based worker"""
        while not self._stop_event.is_set():
            # Check if load is paused due to high CPU
            if self._load_paused:
                time.sleep(1)
                continue
                
            target_concurrency = self._get_current_concurrency(query_config)
            current_concurrency = self.monitor.concurrent_requests[query_config.name]
            
            if current_concurrency < target_concurrency:
                threading.Thread(
                    target=self._execute_query_async,
                    args=(query_config,),
                    daemon=True
                ).start()
            
            time.sleep(0.1)  # Check concurrency every 100ms
    
    def _execute_query_async(self, query_config: QueryConfig):
        """Execute query asynchronously to allow overlapping requests"""
        if self._stop_event.is_set():
            return  # Skip if shutdown initiated
            
        self.monitor.start_request(query_config.name, query_config.query_group)
        start_time = time.time()
        try:
            self._execute_query(query_config)
            duration = time.time() - start_time
            self.metrics.record(duration, True, query_config.name)
            self.monitor.record_request(query_config.name, True)
            self.wal_logger.log(query_config.name, "SUCCESS", duration)
        except Exception as e:
            duration = time.time() - start_time
            # Don't log connection errors during shutdown
            if not self._stop_event.is_set():
                self.metrics.record(duration, False, query_config.name, str(e))
                self.monitor.record_request(query_config.name, False)
                self.wal_logger.log(query_config.name, "ERROR", duration, str(e))
        finally:
            self.monitor.end_request(query_config.name, query_config.query_group)
    
    def _execute_query(self, query_config: QueryConfig):
        """Execute a single query based on its type"""
        if query_config.query_type == QueryType.DSL:
            return self.client.search(
                index=query_config.index,
                body=json.loads(query_config.query),
                timeout='30s'  # Add timeout to prevent hanging
            )
        elif query_config.query_type == QueryType.PPL:
            return self.client.transport.perform_request(
                'POST',
                '/_plugins/_ppl',
                body={"query": query_config.query},
                timeout=30  # Add timeout to prevent hanging
            )
    
    def _get_current_qps(self, query_config: QueryConfig) -> float:
        """Get current QPS based on ramping configuration"""
        if isinstance(query_config.target_qps, (int, float)):
            return query_config.target_qps
        
        # QPS ramping
        elapsed = time.time() - self.test_start_time
        cumulative_time = 0
        
        for ramp in query_config.target_qps:
            if elapsed <= cumulative_time + ramp.duration_seconds:
                return ramp.qps
            cumulative_time += ramp.duration_seconds
        
        # Return last QPS if test runs longer than ramp schedule
        return query_config.target_qps[-1].qps if query_config.target_qps else 0
    
    def _get_max_qps(self, query_config: QueryConfig) -> float:
        """Get maximum QPS for worker calculation"""
        if isinstance(query_config.target_qps, (int, float)):
            return query_config.target_qps
        return max(ramp.qps for ramp in query_config.target_qps) if query_config.target_qps else 0
    
    def _get_max_concurrency(self, query_config: QueryConfig) -> int:
        """Get maximum concurrency for worker calculation"""
        if isinstance(query_config.target_concurrency, int):
            return query_config.target_concurrency
        return max(ramp.concurrency for ramp in query_config.target_concurrency) if query_config.target_concurrency else 1
    
    def _get_current_concurrency(self, query_config: QueryConfig) -> int:
        """Get current target concurrency based on ramping"""
        if isinstance(query_config.target_concurrency, int):
            return query_config.target_concurrency
        
        elapsed = time.time() - self.test_start_time
        cumulative_time = 0
        
        for ramp in query_config.target_concurrency:
            if elapsed <= cumulative_time + ramp.duration_seconds:
                return ramp.concurrency
            cumulative_time += ramp.duration_seconds
        
        return query_config.target_concurrency[-1].concurrency if query_config.target_concurrency else 1
    
    def _print_qps_timeline(self):
        """Print timeline showing only when load changes occur"""
        # Collect all change points from QPS queries
        qps_queries = [q for q in self.config.queries if q.load_mode == LoadMode.QPS]
        change_points = set([0])  # Always start at 0
        
        for query in qps_queries:
            if isinstance(query.target_qps, list):
                cumulative_time = 0
                for ramp in query.target_qps:
                    change_points.add(cumulative_time)
                    cumulative_time += ramp.duration_seconds
                    change_points.add(cumulative_time)
        
        if qps_queries and change_points:
            print("\n=== Target QPS Timeline ===")
            change_points = sorted([t for t in change_points if t <= self.config.duration_seconds])
            
            print(f"{'Time':<12}Total QPS")
            print("-" * 22)
            
            for t in change_points:
                total_qps = sum(self._get_qps_at_time(q, t) for q in qps_queries)
                time_label = f"{t}s start" if t == 0 else f"{t}s"
                print(f"{time_label:<12}{total_qps:>7.1f}")
        
        # Print concurrency timeline for concurrency-based queries
        concurrency_queries = [q for q in self.config.queries if q.load_mode == LoadMode.CONCURRENCY]
        if concurrency_queries:
            # Collect change points from concurrency queries
            change_points = set([0])
            
            for query in concurrency_queries:
                if isinstance(query.target_concurrency, list):
                    cumulative_time = 0
                    for ramp in query.target_concurrency:
                        change_points.add(cumulative_time)
                        cumulative_time += ramp.duration_seconds
                        change_points.add(cumulative_time)
            
            change_points = sorted([t for t in change_points if t <= self.config.duration_seconds])
            
            print("\n=== Target Concurrency Timeline ===")
            
            # Get unique groups
            from ..utils.query_groups import QueryGroup
            groups = list(set(q.query_group for q in concurrency_queries if q.query_group))
            groups.sort(key=lambda g: g.value)
            
            print(f"{'Time':<12}", end="")
            for group in groups:
                print(f"{group.value:<12}", end="")
            print("Total")
            
            print("-" * (12 + 12 * len(groups) + 8))
            
            for t in change_points:
                total_concurrency = 0
                time_label = f"{t}s start" if t == 0 else f"{t}s"
                print(f"{time_label:<12}", end="")
                
                for group in groups:
                    group_concurrency = sum(
                        self._get_concurrency_at_time(q, t) 
                        for q in concurrency_queries if q.query_group == group
                    )
                    total_concurrency += group_concurrency
                    print(f"{group_concurrency:>11}", end="")
                
                print(f"{total_concurrency:>7}")
        
        print()
    
    def _get_qps_at_time(self, query_config: QueryConfig, time_seconds: float) -> float:
        """Get target QPS for a query at specific time"""
        if query_config.load_mode == LoadMode.CONCURRENCY:
            return 0  # Concurrency-based queries don't have QPS targets
        
        if isinstance(query_config.target_qps, (int, float)):
            return query_config.target_qps
        
        if not query_config.target_qps:
            return 0
        
        cumulative_time = 0
        for ramp in query_config.target_qps:
            if time_seconds <= cumulative_time + ramp.duration_seconds:
                return ramp.qps
            cumulative_time += ramp.duration_seconds
        
        return query_config.target_qps[-1].qps
    
    def _get_concurrency_at_time(self, query_config: QueryConfig, time_seconds: float) -> int:
        """Get target concurrency for a query at specific time"""
        if query_config.load_mode == LoadMode.QPS:
            return 0  # QPS-based queries don't have concurrency targets
        
        if isinstance(query_config.target_concurrency, int):
            return query_config.target_concurrency
        
        if not query_config.target_concurrency:
            return 0
        
        cumulative_time = 0
        for ramp in query_config.target_concurrency:
            if time_seconds <= cumulative_time + ramp.duration_seconds:
                return ramp.concurrency
            cumulative_time += ramp.duration_seconds
        
        return query_config.target_concurrency[-1].concurrency
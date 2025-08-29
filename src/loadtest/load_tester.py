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
from ..utils.cluster_monitor import ClusterMonitor

class LoadTester:
    def __init__(self, config: LoadTestConfig):
        print("LoadTester.__init__: Starting...")
        self.config = config
        print("LoadTester.__init__: Storing client config for thread-local clients...")
        self.client_config = config.to_client_config()
        self.client = OpenSearch(**self.client_config)  # Main client for connection testing
        self._thread_local = threading.local()  # Thread-local storage for clients
        print("LoadTester.__init__: Creating metrics collector...")
        self.metrics = MetricsCollector()
        print("LoadTester.__init__: Creating stop event...")
        self._stop_event = threading.Event()
        print("LoadTester.__init__: Generating execution ID...")
        self.execution_id = str(uuid.uuid4())[:8]
        print("LoadTester.__init__: Creating metrics client...")
        # Create separate client for metrics export
        metrics_client = OpenSearch(**config.to_metrics_client_config())
        print("LoadTester.__init__: Creating WAL logger...")
        self.wal_logger = WALLogger(self.execution_id, metrics_client=metrics_client)
        print("LoadTester.__init__: Creating monitor...")
        self.monitor = ObservabilityMonitor()
        print("LoadTester.__init__: Setting flags...")
        self._shutdown_initiated = False
        self._load_paused = False
        print("LoadTester.__init__: Creating cluster monitor (monitoring only)...")
        self.cluster_monitor = ClusterMonitor(self.client, cpu_threshold=80.0, check_interval=10)  # Cluster CPU monitor
        
        print("LoadTester.__init__: Creating metrics exporter...")
        self.metrics_exporter = MetricsExporter(self.client, metrics_client, self.execution_id, self.monitor)
        
        print("LoadTester.__init__: Setting up signal handlers...")
        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
        print("LoadTester.__init__: Setting up cluster monitor callbacks (logging only)...")
        self.cluster_monitor.set_callbacks(self._log_cluster_high, self._log_cluster_normal)
        print("LoadTester.__init__: Complete")
    
    def print_execution_plan(self):
        """Print the execution plan before running tests"""
        print("\n=== Load Test Execution Plan ===")
        if self.config.warmup_enabled:
            print(f"Warmup: {self.config.warmup_duration_seconds} seconds")
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
    
    def _log_cluster_high(self, high_cpu_nodes):
        """Log high cluster CPU usage (no pausing)"""
        node_info = ", ".join([f"{ntype}:{name}:{cpu}%" for ntype, name, cpu in high_cpu_nodes])
        print(f"\n[Cluster Monitor] High CPU detected - {node_info} - MONITORING ONLY")
        self.wal_logger.log("EXECUTION", "CLUSTER_CPU_HIGH")
    
    def _log_cluster_normal(self, max_cpu):
        """Log normal cluster CPU usage"""
        print(f"\n[Cluster Monitor] CPU normalized (max: {max_cpu}%) - NORMAL")
        self.wal_logger.log("EXECUTION", "CLUSTER_CPU_NORMAL")
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully"""
        if not self._shutdown_initiated:
            self._shutdown_initiated = True
            print(f"\n\nReceived signal {signum}. Initiating graceful shutdown...")
            self._stop_event.set()
            self.monitor.stop_monitoring()
            self.cluster_monitor.stop_monitoring()
            self.wal_logger.log("EXECUTION", "INTERRUPTED")
    
    def run_test(self):
        """Execute load test with configured queries"""
        self.print_execution_plan()
        
        # Test connection before starting load test
        print(f"\nTesting connection to {self.config.host}:{self.config.port}...")
        try:
            info = self.client.info()
            print(f"✓ Connected to OpenSearch {info['version']['number']} - Cluster: {info['cluster_name']}")
        except Exception as e:
            print(f"✗ Connection failed: {e}")
            print("Please check your connection settings and try again.")
            return {"overall": {"total": 0, "success": 0, "errors": 1, "success_rate": 0.0, "avg_duration": 0.0, "rps": 0.0}}
        
        print(f"\nStarting load test... [Execution ID: {self.execution_id}]")
        print("Cluster CPU monitoring: logging only (no pausing)")
        self.wal_logger.log("EXECUTION", "START")
        self._log_timeline()
        self.monitor.start_monitoring()
        self.cluster_monitor.start_monitoring()
        self.metrics_exporter.export_node_stats()
        self.test_start_time = time.time()
        

        
        # Calculate max workers based on concurrency targets
        max_concurrent = sum(self._get_max_concurrency(q) for q in self.config.queries)
        max_workers = max(50, max_concurrent + 20)  # Buffer for QPS queries
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Run warmup phase if enabled
            if self.config.warmup_enabled:
                self._run_warmup_phase(executor)
            
            # Submit worker threads for each query
            futures = []
            for query_config in self.config.queries:
                # For concurrency mode, use max concurrency workers
                # For QPS mode, use 1 worker per query
                if query_config.load_mode == LoadMode.CONCURRENCY:
                    workers_needed = self._get_max_concurrency(query_config)
                else:
                    workers_needed = 1
                
                for worker_id in range(workers_needed):
                    future = executor.submit(self._query_worker, query_config, worker_id, workers_needed)
                    futures.append(future)
            
            # Wait for test duration with frequent metrics collection
            start_time = time.time()
            last_metrics_time = start_time
            
            while time.time() - start_time < self.config.duration_seconds:
                current_time = time.time()
                
                # Export node stats every 0.5 seconds
                if current_time - last_metrics_time >= 0.5:
                    self.metrics_exporter.export_node_stats()
                    last_metrics_time = current_time
                
                time.sleep(0.1)  # Check every 100ms for precise timing
            
            if not self._shutdown_initiated:
                self._stop_event.set()
                self.monitor.stop_monitoring()
                self.cluster_monitor.stop_monitoring()
            
            # Final metrics export and flush
            try:
                self.metrics_exporter.export_node_stats()
                self.metrics_exporter.flush_pending_metrics()
                self.wal_logger.flush_pending_metrics()
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
        # Start with random offset to avoid all queries starting at once
        import random
        current_qps = self._get_current_qps(query_config)
        if current_qps > 0:
            initial_delay = random.uniform(0, 1.0 / current_qps) if current_qps < 1 else random.uniform(0, 1.0)
            time.sleep(initial_delay)
        
        next_request_time = time.time()
        
        while not self._stop_event.is_set():
            current_qps = self._get_current_qps(query_config)
            if current_qps <= 0:
                time.sleep(1)
                continue
            
            worker_qps = current_qps / total_workers
            interval = 1.0 / worker_qps if worker_qps > 0 else 100.0
            
            current_time = time.time()
            if current_time < next_request_time:
                time.sleep(next_request_time - current_time)
            
            next_request_time = time.time() + interval
            
            # Execute query regardless of CPU (no pausing)
            self._execute_query_async(query_config)
    
    def _concurrency_worker(self, query_config: QueryConfig):
        """Concurrency-based worker - each worker maintains 1 concurrent request"""
        while not self._stop_event.is_set():
            target_concurrency = self._get_current_concurrency(query_config)
            
            # Each worker represents 1 concurrent request
            # Only execute if this worker should be active based on current target
            worker_should_be_active = target_concurrency > 0
            
            if worker_should_be_active:
                self._execute_query_async(query_config)
            else:
                time.sleep(0.1)  # Wait if not active
    
    def _execute_query_async(self, query_config: QueryConfig):
        """Execute query asynchronously to allow overlapping requests"""
        if self._stop_event.is_set():
            return  # Skip if shutdown initiated
            
        self.monitor.start_request(query_config.name, query_config.query_group)
        start_time = time.time()
        try:
            print(f"[START] Executing {query_config.name}...")
            self._execute_query(query_config)
            duration_ms = (time.time() - start_time) * 1000
            self.metrics.record(duration_ms, True, query_config.name)
            self.monitor.record_request(query_config.name, True)
            self.wal_logger.log(query_config.name, "SUCCESS", duration_ms)
            self.metrics_exporter.export_query_metrics(query_config.name, duration_ms)
            print(f"[SUCCESS] {query_config.name} completed in {duration_ms:.1f}ms")
        except Exception as e:
            duration_ms = (time.time() - start_time) * 1000
            # Don't log connection errors during shutdown
            if not self._stop_event.is_set():
                self.metrics.record(duration_ms, False, query_config.name, str(e))
                self.monitor.record_request(query_config.name, False)
                self.wal_logger.log(query_config.name, "ERROR", duration_ms, str(e))
                self.metrics_exporter.export_query_metrics(query_config.name, duration_ms)
                print(f"[ERROR] {query_config.name} failed in {duration_ms:.1f}ms: {str(e)[:100]}")
        finally:
            self.monitor.end_request(query_config.name, query_config.query_group)
    
    def _get_thread_client(self):
        """Get or create OpenSearch client for current thread"""
        if not hasattr(self._thread_local, 'client'):
            self._thread_local.client = OpenSearch(**self.client_config)
        return self._thread_local.client
    
    def _execute_query(self, query_config: QueryConfig):
        """Execute a single query based on its type using thread-local client"""
        client = self._get_thread_client()
        if query_config.query_type == QueryType.DSL:
            return client.search(
                index=query_config.index,
                body=json.loads(query_config.query),
                timeout=300  # 300s timeout
            )
        elif query_config.query_type == QueryType.PPL:
            return client.transport.perform_request(
                'POST',
                '/_plugins/_ppl',
                body={"query": query_config.query},
                timeout=300  # 300s timeout
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
    
    def _run_warmup_phase(self, executor):
        """Run warmup phase with same concurrency for all queries"""
        print(f"\n=== Starting Warmup Phase ({self.config.warmup_duration_seconds}s) ===")
        self.wal_logger.log("EXECUTION", "WARMUP_START")
        
        # Create warmup queries with same concurrency for all
        warmup_queries = []
        for query_config in self.config.queries:
            warmup_query = QueryConfig(
                name=f"warmup_{query_config.name}",
                query_type=query_config.query_type,
                query=query_config.query,
                load_mode=LoadMode.CONCURRENCY,
                target_concurrency=1,  # Same concurrency for all queries
                index=query_config.index,
                query_group=query_config.query_group
            )
            warmup_queries.append(warmup_query)
        
        # Submit warmup workers
        warmup_futures = []
        for warmup_query in warmup_queries:
            future = executor.submit(self._warmup_worker, warmup_query)
            warmup_futures.append(future)
        
        # Wait for warmup duration
        warmup_start = time.time()
        while time.time() - warmup_start < self.config.warmup_duration_seconds:
            if self._stop_event.is_set():
                break
            time.sleep(1)
        
        # Stop warmup workers
        self._warmup_stop_event = threading.Event()
        self._warmup_stop_event.set()
        
        # Wait for warmup workers to complete
        for future in warmup_futures:
            try:
                future.result(timeout=10)
            except Exception as e:
                print(f"Warmup worker error: {e}")
        
        print("=== Warmup Phase Complete ===")
        self.wal_logger.log("EXECUTION", "WARMUP_END")
        
        # Reset test start time for main phase
        self.test_start_time = time.time()
    
    def _warmup_worker(self, query_config: QueryConfig):
        """Warmup worker that runs at fixed concurrency"""
        self._warmup_stop_event = getattr(self, '_warmup_stop_event', threading.Event())
        
        while not self._warmup_stop_event.is_set() and not self._stop_event.is_set():
            
            # Execute query with minimal logging during warmup
            self.monitor.start_request(query_config.name, query_config.query_group)
            start_time = time.time()
            try:
                self._execute_query(query_config)
                duration_ms = (time.time() - start_time) * 1000
                # Don't record warmup metrics in main metrics collector
                self.wal_logger.log(query_config.name, "WARMUP_SUCCESS", duration_ms)
            except Exception as e:
                duration_ms = (time.time() - start_time) * 1000
                if not self._stop_event.is_set():
                    self.wal_logger.log(query_config.name, "WARMUP_ERROR", duration_ms, str(e))
            finally:
                self.monitor.end_request(query_config.name, query_config.query_group)
    
    def _log_timeline(self):
        """Log the execution timeline to a separate log file"""
        timeline_file = f"logs/{self.execution_id}_TIMELINE.log"
        
        timeline_data = {
            "execution_id": self.execution_id,
            "timestamp": time.time(),
            "duration_seconds": self.config.duration_seconds,
            "queries": []
        }
        
        for query in self.config.queries:
            query_data = {
                "name": query.name,
                "load_mode": query.load_mode.value,
                "query_group": query.query_group.value if query.query_group else None
            }
            
            if query.load_mode == LoadMode.QPS:
                if isinstance(query.target_qps, (int, float)):
                    query_data["target_qps"] = query.target_qps
                else:
                    query_data["qps_ramp"] = [{"qps": r.qps, "duration": r.duration_seconds} for r in query.target_qps]
            else:
                if isinstance(query.target_concurrency, int):
                    query_data["target_concurrency"] = query.target_concurrency
                else:
                    query_data["concurrency_ramp"] = [{"concurrency": r.concurrency, "duration": r.duration_seconds} for r in query.target_concurrency]
            
            timeline_data["queries"].append(query_data)
        
        try:
            with open(timeline_file, 'w') as f:
                f.write(json.dumps(timeline_data, indent=2))
        except Exception as e:
            print(f"Warning: Failed to write timeline log: {e}")
    
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
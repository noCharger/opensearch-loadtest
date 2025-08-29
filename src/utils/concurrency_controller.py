import threading
import time
import queue
from typing import Dict, List, Callable
from src.utils.query_groups import QueryGroup

class ConcurrencyController:
    """Maintains constant concurrency per query group"""
    
    def __init__(self, execute_query_func: Callable):
        self.execute_query_func = execute_query_func
        self.active_workers: Dict[QueryGroup, List[threading.Thread]] = {}
        self.target_concurrency: Dict[QueryGroup, int] = {}
        self.query_queues: Dict[QueryGroup, queue.Queue] = {}
        self.running = False
        self.lock = threading.Lock()
        
    def set_concurrency(self, group: QueryGroup, concurrency: int, queries: List[str]):
        """Set target concurrency for a query group"""
        with self.lock:
            self.target_concurrency[group] = concurrency
            
            # Initialize queue if not exists
            if group not in self.query_queues:
                self.query_queues[group] = queue.Queue()
                self.active_workers[group] = []
            
            # Fill queue with queries
            for query in queries:
                self.query_queues[group].put(query)
            
            # Adjust worker count
            self._adjust_workers(group)
    
    def _adjust_workers(self, group: QueryGroup):
        """Adjust number of workers for a group"""
        current_count = len(self.active_workers[group])
        target_count = self.target_concurrency.get(group, 0)
        
        if target_count > current_count:
            # Start new workers
            for _ in range(target_count - current_count):
                worker = threading.Thread(target=self._worker_loop, args=(group,))
                worker.daemon = True
                self.active_workers[group].append(worker)
                worker.start()
        
        elif target_count < current_count:
            # Stop excess workers by putting None in queue
            for _ in range(current_count - target_count):
                self.query_queues[group].put(None)
    
    def _worker_loop(self, group: QueryGroup):
        """Worker thread that continuously executes queries"""
        while self.running:
            try:
                # Get next query from queue
                query = self.query_queues[group].get(timeout=1.0)
                
                # None signals worker to stop
                if query is None:
                    break
                
                # Execute query - no pausing, always maintain concurrency
                self.execute_query_func(query, group)
                
                # Put query back in queue for continuous execution
                self.query_queues[group].put(query)
                
            except queue.Empty:
                continue
            except Exception as e:
                # Log error but continue running to maintain concurrency
                print(f"Worker error in group {group}: {e}")
        
        # Remove self from active workers
        with self.lock:
            if threading.current_thread() in self.active_workers[group]:
                self.active_workers[group].remove(threading.current_thread())
    
    def start(self):
        """Start the concurrency controller"""
        self.running = True
    
    def stop(self):
        """Stop all workers"""
        self.running = False
        
        # Signal all workers to stop
        for group in self.query_queues:
            for _ in self.active_workers[group]:
                self.query_queues[group].put(None)
        
        # Wait for workers to finish
        for group in self.active_workers:
            for worker in self.active_workers[group]:
                worker.join(timeout=5.0)
    
    def get_active_count(self, group: QueryGroup) -> int:
        """Get current active worker count for a group"""
        return len(self.active_workers.get(group, []))
    
    def update_concurrency(self, group: QueryGroup, new_concurrency: int):
        """Update concurrency for a group during runtime"""
        with self.lock:
            old_concurrency = self.target_concurrency.get(group, 0)
            self.target_concurrency[group] = new_concurrency
            
            if new_concurrency != old_concurrency:
                self._adjust_workers(group)
                print(f"Updated {group} concurrency: {old_concurrency} -> {new_concurrency}")
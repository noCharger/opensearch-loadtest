import threading
import time
from typing import Callable, Optional
from opensearchpy import OpenSearch

class ClusterMonitor:
    def __init__(self, client: OpenSearch, cpu_threshold: float = 80.0, check_interval: int = 10):
        self.client = client
        self.cpu_threshold = cpu_threshold
        self.check_interval = check_interval
        self.is_paused = False
        self.running = False
        self.monitor_thread: Optional[threading.Thread] = None
        self.pause_callback: Optional[Callable] = None
        self.resume_callback: Optional[Callable] = None
        self.lock = threading.Lock()
    
    def set_callbacks(self, pause_callback: Callable, resume_callback: Callable):
        """Set callbacks for pause and resume events"""
        self.pause_callback = pause_callback
        self.resume_callback = resume_callback
    
    def start_monitoring(self):
        """Start cluster monitoring in background thread"""
        self.running = True
        self.monitor_thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self.monitor_thread.start()
    
    def stop_monitoring(self):
        """Stop cluster monitoring"""
        self.running = False
        if self.monitor_thread:
            self.monitor_thread.join(timeout=1)
    
    def _monitor_loop(self):
        """Main monitoring loop"""
        while self.running:
            try:
                # Get node info and stats
                nodes_info = self.client.nodes.info()
                stats = self.client.nodes.stats(metric='os')
                nodes_stats = stats.get('nodes', {})
                
                node_types = {'data': [], 'master': [], 'ingest': [], 'coordinating': []}
                
                # Monitor only data nodes
                for node_id, node_stats in nodes_stats.items():
                    if not isinstance(node_stats, dict) or 'os' not in node_stats:
                        continue
                        
                    roles = nodes_info.get('nodes', {}).get(node_id, {}).get('roles', [])
                    
                    # Only monitor data nodes
                    if 'data' not in roles:
                        continue
                        
                    cpu_percent = node_stats['os'].get('cpu', {}).get('percent', 0)
                    node_name = nodes_info.get('nodes', {}).get(node_id, {}).get('name', node_id[:8])
                    node_types['data'].append((node_name, cpu_percent))
                
                # Check CPU thresholds for data nodes
                high_cpu_nodes = []
                max_cpu = 0
                
                data_nodes = node_types['data']
                if data_nodes:
                    max_cpu = max(cpu for _, cpu in data_nodes)
                    
                    for node_name, cpu in data_nodes:
                        if cpu > self.cpu_threshold:
                            high_cpu_nodes.append(('data', node_name, cpu))
                
                with self.lock:
                    if high_cpu_nodes and not self.is_paused:
                        self.is_paused = True
                        if self.pause_callback:
                            self.pause_callback(high_cpu_nodes)
                    
                    elif max_cpu <= self.cpu_threshold - 10 and self.is_paused:
                        self.is_paused = False
                        if self.resume_callback:
                            self.resume_callback(max_cpu)
                
                time.sleep(self.check_interval)
                
            except Exception as e:
                print(f"Cluster monitoring error: {e}")
                time.sleep(self.check_interval)
    
    def should_pause(self) -> bool:
        """Check if load generation should be paused"""
        with self.lock:
            return self.is_paused
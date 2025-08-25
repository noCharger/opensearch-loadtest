import psutil
import threading
import time
from typing import Callable, Optional

class CPUMonitor:
    def __init__(self, threshold: float = 90.0, check_interval: int = 5):
        self.threshold = threshold
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
        """Start CPU monitoring in background thread"""
        self.running = True
        self.monitor_thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self.monitor_thread.start()
    
    def stop_monitoring(self):
        """Stop CPU monitoring"""
        self.running = False
        if self.monitor_thread:
            self.monitor_thread.join(timeout=1)
    
    def _monitor_loop(self):
        """Main monitoring loop"""
        while self.running:
            try:
                cpu_percent = psutil.cpu_percent(interval=1)
                
                with self.lock:
                    if cpu_percent > self.threshold and not self.is_paused:
                        self.is_paused = True
                        print(f"\\n[CPU Monitor] CPU usage {cpu_percent:.1f}% > {self.threshold}% - PAUSING load generation")
                        if self.pause_callback:
                            self.pause_callback()
                    
                    elif cpu_percent <= self.threshold - 5 and self.is_paused:  # 5% hysteresis
                        self.is_paused = False
                        print(f"\\n[CPU Monitor] CPU usage {cpu_percent:.1f}% - RESUMING load generation")
                        if self.resume_callback:
                            self.resume_callback()
                
                time.sleep(self.check_interval)
                
            except Exception as e:
                print(f"CPU monitoring error: {e}")
                time.sleep(self.check_interval)
    
    def should_pause(self) -> bool:
        """Check if load generation should be paused"""
        with self.lock:
            return self.is_paused
import os
import json
import time
import threading
from dataclasses import dataclass
from typing import Dict, Any

@dataclass
class WALEntry:
    timestamp: float
    execution_id: str
    query_name: str
    event_type: str
    duration: float = None
    error: str = None

class WALLogger:
    def __init__(self, execution_id: str, log_dir: str = "logs"):
        self.execution_id = execution_id
        self.log_dir = log_dir
        self.locks = {}
        os.makedirs(log_dir, exist_ok=True)
    
    def _get_log_file(self, query_name: str) -> str:
        return os.path.join(self.log_dir, f"{self.execution_id}_{query_name}.log")
    
    def _get_lock(self, query_name: str) -> threading.Lock:
        if query_name not in self.locks:
            self.locks[query_name] = threading.Lock()
        return self.locks[query_name]
    
    def log(self, query_name: str, event_type: str, duration: float = None, error: str = None):
        entry = WALEntry(
            timestamp=time.time(),
            execution_id=self.execution_id,
            query_name=query_name,
            event_type=event_type,
            duration=duration,
            error=error
        )
        
        log_file = self._get_log_file(query_name)
        lock = self._get_lock(query_name)
        
        with lock:
            with open(log_file, 'a') as f:
                f.write(json.dumps(entry.__dict__) + '\n')